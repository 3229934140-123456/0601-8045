"""
预写日志 (Write-Ahead Logging) 引擎实现

支持事务回滚、崩溃恢复、检查点机制

WAL 核心原则:
1. 日志记录的刷盘时机必须严格早于对应数据页的刷盘
2. 恢复时通过 COMMIT 标记区分重做/撤销
3. 检查点避免全量重放并保证原子写入
"""

import os
import json
import pickle
import struct
import hashlib
import threading
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import copy


class LogRecordType(Enum):
    """日志记录类型"""
    BEGIN = 0          # 事务开始
    UPDATE = 1         # 数据更新 (包含前后镜像)
    COMMIT = 2         # 事务提交
    ROLLBACK = 3       # 事务回滚
    CHECKPOINT_BEGIN = 4  # 检查点开始
    CHECKPOINT_END = 5    # 检查点结束
    CLR = 6            # 补偿日志记录 (Compensation Log Record)


@dataclass
class LogRecord:
    """日志记录"""
    lsn: int                    # 日志序列号 (Log Sequence Number)
    txn_id: int                 # 事务ID
    record_type: LogRecordType   # 记录类型
    prev_lsn: int              # 同一事务的前一条日志LSN
    page_id: Optional[int] = None    # 涉及的数据页ID (UPDATE类型使用)
    before_image: Optional[Dict[str, Any]] = None  # 修改前的数据镜像 (用于UNDO)
    after_image: Optional[Dict[str, Any]] = None   # 修改后的数据镜像 (用于REDO)
    undo_next_lsn: Optional[int] = None  # CLR专用: 下一个需要撤销的LSN (跳过已撤销的操作)
    checkpoint_lsns: Optional[List[int]] = None  # 检查点相关LSN列表 (CHECKPOINT_END使用)
    checksum: int = 0            # 校验和

    def serialize(self) -> bytes:
        """序列化日志记录为二进制"""
        data = {
            'lsn': self.lsn,
            'txn_id': self.txn_id,
            'record_type': self.record_type.value,
            'prev_lsn': self.prev_lsn,
            'page_id': self.page_id,
            'before_image': self.before_image,
            'after_image': self.after_image,
            'undo_next_lsn': self.undo_next_lsn,
            'checkpoint_lsns': self.checkpoint_lsns,
        }
        payload = pickle.dumps(data)
        length = len(payload)
        checksum = hashlib.md5(payload).digest()
        header = struct.pack('<IQ', length, self.lsn)
        return header + checksum + payload

    @staticmethod
    def deserialize(data: bytes) -> 'LogRecord':
        """从二进制反序列化日志记录"""
        header_size = struct.calcsize('<IQ')
        length, lsn = struct.unpack('<IQ', data[:header_size])
        checksum_stored = data[header_size:header_size + 16]
        payload = data[header_size + 16:header_size + 16 + length]
        checksum_calc = hashlib.md5(payload).digest()
        if checksum_stored != checksum_calc:
            raise ValueError(f"日志记录校验和失败, LSN={lsn}")
        obj = pickle.loads(payload)
        return LogRecord(
            lsn=obj['lsn'],
            txn_id=obj['txn_id'],
            record_type=LogRecordType(obj['record_type']),
            prev_lsn=obj['prev_lsn'],
            page_id=obj['page_id'],
            before_image=obj['before_image'],
            after_image=obj['after_image'],
            undo_next_lsn=obj.get('undo_next_lsn'),
            checkpoint_lsns=obj['checkpoint_lsns'],
            checksum=int.from_bytes(checksum_stored, 'little')
        )


@dataclass
class TransactionTableEntry:
    """事务表条目"""
    txn_id: int
    status: str  # 'active', 'committed', 'aborted'
    last_lsn: int  # 该事务最后一条日志的LSN


@dataclass
class DirtyPageTableEntry:
    """脏页表条目"""
    page_id: int
    rec_lsn: int  # 该页首次变脏时对应的LSN (recovery LSN)


class PageStore:
    """
    数据页存储 - 模拟实际的数据页存储

    使用简单的文件持久化存储数据页
    每个数据页是一个独立的文件
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._page_cache: Dict[int, Dict[str, Any]] = {}
        self._page_dirty: Dict[int, bool] = {}
        self._page_lsn: Dict[int, int] = {}

    def read_page(self, page_id: int) -> Dict[str, Any]:
        """读取数据页"""
        if page_id in self._page_cache:
            return copy.deepcopy(self._page_cache[page_id])
        page_file = os.path.join(self.data_dir, f'page_{page_id}.dat')
        if os.path.exists(page_file):
            with open(page_file, 'rb') as f:
                data = pickle.load(f)
                self._page_cache[page_id] = data
                self._page_dirty[page_id] = False
                return copy.deepcopy(data)
        return {'id': page_id, 'data': {}}

    def write_page(self, page_id: int, data: Dict[str, Any], lsn: int):
        """写入数据页到缓存（不立即刷盘）"""
        self._page_cache[page_id] = copy.deepcopy(data)
        self._page_dirty[page_id] = True
        self._page_lsn[page_id] = lsn

    def flush_page(self, page_id: int) -> bool:
        """将指定数据页刷盘，返回是否成功"""
        if not self._page_dirty.get(page_id, False):
            return False
        page_file = os.path.join(self.data_dir, f'page_{page_id}.dat')
        tmp_file = page_file + '.tmp'
        with open(tmp_file, 'wb') as f:
            pickle.dump(self._page_cache[page_id], f)
        os.replace(tmp_file, page_file)
        self._page_dirty[page_id] = False
        return True

    def flush_all_pages(self):
        """将所有脏页刷盘"""
        for page_id in list(self._page_dirty.keys()):
            if self._page_dirty[page_id]:
                self.flush_page(page_id)

    def get_dirty_pages(self) -> List[int]:
        """获取所有脏页"""
        return [pid for pid, dirty in self._page_dirty.items() if dirty]

    def get_page_lsn(self, page_id: int) -> int:
        """获取页面对应的LSN"""
        return self._page_lsn.get(page_id, 0)

    def is_dirty(self, page_id: int) -> bool:
        """检查页面是否为脏"""
        return self._page_dirty.get(page_id, False)


class WALManager:
    """
    预写日志管理器

    核心职责:
    1. 顺序追加日志记录
    2. 保证日志刷盘先于数据页刷盘 (WAL原则)
    3. 管理事务状态
    4. 崩溃恢复
    5. 检查点机制
    """

    HEADER_SIZE = 4096  # WAL文件头部大小

    def __init__(self, wal_dir: str, data_dir: str):
        self.wal_dir = wal_dir
        self.data_dir = data_dir
        os.makedirs(wal_dir, exist_ok=True)

        self.wal_file = os.path.join(wal_dir, 'wal.log')
        self.checkpoint_file = os.path.join(wal_dir, 'checkpoint.dat')
        self.master_record_file = os.path.join(wal_dir, 'master.dat')

        self.page_store = PageStore(data_dir)

        self._lock = threading.RLock()
        self._current_lsn = 0
        self._current_txn_id = 0

        # 事务表: txn_id -> TransactionTableEntry
        self._transaction_table: Dict[int, TransactionTableEntry] = {}

        # 脏页表: page_id -> DirtyPageTableEntry
        self._dirty_page_table: Dict[int, DirtyPageTableEntry] = {}

        # 活跃事务的prev_lsn追踪
        self._txn_prev_lsn: Dict[int, int] = {}

        # 已刷盘的LSN
        self._flushed_lsn = 0

        # 检查点LSN
        self._checkpoint_lsn = 0

        self._init_wal()

    def _init_wal(self):
        """初始化WAL系统"""
        if not os.path.exists(self.wal_file):
            self._create_wal_file()
        self._load_master_record()

    def _create_wal_file(self):
        """创建新的WAL文件"""
        with open(self.wal_file, 'wb') as f:
            f.write(b'\x00' * self.HEADER_SIZE)
        self._save_master_record()

    def _save_master_record(self):
        """保存主记录 (包含检查点LSN等关键信息)"""
        master_data = {
            'checkpoint_lsn': self._checkpoint_lsn,
            'current_lsn': self._current_lsn,
        }
        tmp_file = self.master_record_file + '.tmp'
        with open(tmp_file, 'wb') as f:
            pickle.dump(master_data, f)
        os.replace(tmp_file, self.master_record_file)

    def _load_master_record(self):
        """加载主记录"""
        if os.path.exists(self.master_record_file):
            try:
                with open(self.master_record_file, 'rb') as f:
                    data = pickle.load(f)
                    self._checkpoint_lsn = data.get('checkpoint_lsn', 0)
                    self._current_lsn = data.get('current_lsn', 0)
            except Exception:
                pass

    def _allocate_lsn(self) -> int:
        """分配新的LSN"""
        self._current_lsn += 1
        return self._current_lsn

    def _allocate_txn_id(self) -> int:
        """分配新的事务ID"""
        self._current_txn_id += 1
        return self._current_txn_id

    def _append_log(self, record: LogRecord):
        """
        追加日志记录到WAL文件（内存缓冲，需要手动flush）

        注意：这只是写入文件缓冲区，必须调用_flush_wal确保刷盘
        """
        with open(self.wal_file, 'ab') as f:
            f.write(record.serialize())
        self._flushed_lsn = record.lsn

    def _flush_wal(self, up_to_lsn: Optional[int] = None):
        """
        强制刷盘WAL日志到指定LSN

        关键：WAL原则 - 日志必须先于数据页刷盘
        """
        import sys
        with open(self.wal_file, 'ab') as f:
            f.flush()
            os.fsync(f.fileno())
        if up_to_lsn:
            self._flushed_lsn = max(self._flushed_lsn, up_to_lsn)
        self._save_master_record()

    def begin_transaction(self) -> int:
        """
        开始一个新事务"""
        with self._lock:
            txn_id = self._allocate_txn_id()
            lsn = self._allocate_lsn()

            prev_lsn = 0
            record = LogRecord(
                lsn=lsn,
                txn_id=txn_id,
                record_type=LogRecordType.BEGIN,
                prev_lsn=prev_lsn
            )

            self._append_log(record)
            self._flush_wal(lsn)

            self._transaction_table[txn_id] = TransactionTableEntry(
                txn_id=txn_id,
                status='active',
                last_lsn=lsn
            )
            self._txn_prev_lsn[txn_id] = lsn

            return txn_id

    def update(self, txn_id: int, page_id: int,
               before_image: Dict[str, Any],
               after_image: Dict[str, Any]) -> int:
        """
        记录数据更新操作

        遵循WAL原则：
        1. 先写日志（包含前后镜像）
        2. 日志刷盘
        3. 再修改内存中的数据页

        返回分配的LSN
        """
        with self._lock:
            if txn_id not in self._transaction_table:
                raise ValueError(f"事务 {txn_id} 不存在或已结束")

            txn_entry = self._transaction_table[txn_id]
            if txn_entry.status != 'active':
                raise ValueError(f"事务 {txn_id} 状态不是活跃")

            lsn = self._allocate_lsn()
            prev_lsn = self._txn_prev_lsn[txn_id]

            record = LogRecord(
                lsn=lsn,
                txn_id=txn_id,
                record_type=LogRecordType.UPDATE,
                prev_lsn=prev_lsn,
                page_id=page_id,
                before_image=copy.deepcopy(before_image),
                after_image=copy.deepcopy(after_image)
            )

            self._append_log(record)

            self._flush_wal(lsn)

            self.page_store.write_page(page_id, after_image, lsn)

            if page_id not in self._dirty_page_table:
                self._dirty_page_table[page_id] = DirtyPageTableEntry(
                    page_id=page_id,
                    rec_lsn=lsn
                )

            txn_entry.last_lsn = lsn
            self._txn_prev_lsn[txn_id] = lsn

            return lsn

    def commit_transaction(self, txn_id: int):
        """
        提交事务

        提交协议：
        1. 写入COMMIT日志记录
        2. 强制刷盘COMMIT日志 - 这是事务提交的持久化保证
        3. 更新事务状态
        """
        with self._lock:
            if txn_id not in self._transaction_table:
                raise ValueError(f"事务 {txn_id} 不存在")

            txn_entry = self._transaction_table[txn_id]
            if txn_entry.status != 'active':
                return

            lsn = self._allocate_lsn()
            prev_lsn = self._txn_prev_lsn[txn_id]

            commit_record = LogRecord(
                lsn=lsn,
                txn_id=txn_id,
                record_type=txn_id,
                prev_lsn=prev_lsn
            )
            commit_record.record_type = LogRecordType.COMMIT

            self._append_log(commit_record)
            self._flush_wal(lsn)

            txn_entry.status = 'committed'
            txn_entry.last_lsn = lsn
            self._txn_prev_lsn[txn_id] = lsn

            self._save_master_record()

    def abort_transaction(self, txn_id: int):
        """
        回滚事务（主动撤销）

        执行UNDO操作：
        1. 从事务最后一条日志开始反向遍历
        2. 使用before_image恢复数据
        3. 每条UNDO操作记录CLR日志
        4. 写入ROLLBACK日志并刷盘
        """
        with self._lock:
            if txn_id not in self._transaction_table:
                raise ValueError(f"事务 {txn_id} 不存在")

            txn_entry = self._transaction_table[txn_id]
            if txn_entry.status != 'active':
                return

            self._undo_transaction(txn_id)

            lsn = self._allocate_lsn()
            prev_lsn = self._txn_prev_lsn[txn_id]

            rollback_record = LogRecord(
                lsn=lsn,
                txn_id=txn_id,
                record_type=LogRecordType.ROLLBACK,
                prev_lsn=prev_lsn
            )

            self._append_log(rollback_record)
            self._flush_wal(lsn)

            txn_entry.status = 'aborted'
            txn_entry.last_lsn = lsn
            self._txn_prev_lsn[txn_id] = lsn

            self._save_master_record()

    def _undo_transaction(self, txn_id: int):
        """
        执行事务的UNDO操作 (ARIES风格)

        从事务last_lsn开始沿日志链反向遍历:
        - 遇到UPDATE: 写CLR(含undo_next_lsn=该UPDATE的prev_lsn)，用before_image恢复页面
        - 遇到CLR: 跳到其undo_next_lsn继续(之前已撤销的操作不再重复)
        - 遇到BEGIN: 停止

        撤销顺序: 最新操作先撤销，保证同一页多次修改时最终回到事务前原始值
        """
        logs_by_lsn: Dict[int, LogRecord] = {}
        self._scan_wal_from(0, logs_by_lsn)

        if txn_id in self._transaction_table:
            current_lsn = self._transaction_table[txn_id].last_lsn
        else:
            current_lsn = self._txn_prev_lsn.get(txn_id, 0)

        visited = set()
        while current_lsn != 0 and current_lsn not in visited:
            visited.add(current_lsn)
            if current_lsn not in logs_by_lsn:
                break
            record = logs_by_lsn[current_lsn]

            if record.txn_id != txn_id:
                break

            if record.record_type == LogRecordType.UPDATE:
                if record.page_id is not None and record.before_image is not None:
                    clr_lsn = self._allocate_lsn()
                    prev_lsn = self._txn_prev_lsn.get(txn_id, 0)

                    clr_record = LogRecord(
                        lsn=clr_lsn,
                        txn_id=txn_id,
                        record_type=LogRecordType.CLR,
                        prev_lsn=prev_lsn,
                        page_id=record.page_id,
                        before_image=record.after_image,
                        after_image=record.before_image,
                        undo_next_lsn=record.prev_lsn
                    )

                    self._append_log(clr_record)

                    self.page_store.write_page(
                        record.page_id,
                        record.before_image,
                        clr_lsn
                    )

                    self._txn_prev_lsn[txn_id] = clr_lsn

                current_lsn = record.prev_lsn

            elif record.record_type == LogRecordType.CLR:
                current_lsn = record.undo_next_lsn if record.undo_next_lsn is not None else record.prev_lsn

            elif record.record_type == LogRecordType.BEGIN:
                break

            else:
                current_lsn = record.prev_lsn

    def flush_pages_with_wal_guarantee(self, page_id: int):
        """
        遵循WAL原则刷盘数据页

        WAL原则的核心：
        在数据页刷盘前，必须确保该页相关的所有日志记录都已刷盘。
        特别是该页的rec_lsn之前的日志必须已经持久化。
        """
        with self._lock:
            if page_id in self._dirty_page_table:
                rec_lsn = self._dirty_page_table[page_id].rec_lsn
                if self._flushed_lsn < rec_lsn:
                    self._flush_wal(rec_lsn)

            self.page_store.flush_page(page_id)

            if page_id in self._dirty_page_table:
                del self._dirty_page_table[page_id]

    def create_checkpoint(self):
        """
        创建检查点

        步骤：
        1. 写入CHECKPOINT_BEGIN日志
        2. 将所有脏页刷盘（遵循WAL原则）
        3. 保存检查点信息（事务表+脏页表+redo_lsn）
        4. 写入CHECKPOINT_END日志（含检查点信息的LSN列表）
        5. 强制刷盘
        6. 更新主记录中的检查点LSN

        检查点原子性保证：
        - CHECKPOINT_END日志记录了完整的检查点信息
        - 只有看到CHECKPOINT_END才认为检查点有效
        - 主记录的更新使用原子文件替换
        """
        with self._lock:
            begin_lsn = self._allocate_lsn()
            begin_record = LogRecord(
                lsn=begin_lsn,
                txn_id=0,
                record_type=LogRecordType.CHECKPOINT_BEGIN,
                prev_lsn=0
            )
            self._append_log(begin_record)
            self._flush_wal(begin_lsn)

            checkpoint_txn_table = copy.deepcopy(self._transaction_table)

            dirty_pages = list(self._dirty_page_table.keys())
            for page_id in dirty_pages:
                self.flush_pages_with_wal_guarantee(page_id)

            checkpoint_dirty_table = copy.deepcopy(self._dirty_page_table)

            if checkpoint_dirty_table:
                redo_lsn = min(e.rec_lsn for e in checkpoint_dirty_table.values())
            else:
                redo_lsn = begin_lsn + 1

            checkpoint_data = {
                'txn_table': {tid: e.__dict__ for tid, e in checkpoint_txn_table.items()},
                'dirty_page_table': {pid: e.__dict__ for pid, e in checkpoint_dirty_table.items()},
                'redo_lsn': redo_lsn,
                'begin_lsn': begin_lsn,
            }
            tmp_file = self.checkpoint_file + '.tmp'
            with open(tmp_file, 'wb') as f:
                pickle.dump(checkpoint_data, f)
            os.replace(tmp_file, self.checkpoint_file)

            end_lsn = self._allocate_lsn()
            end_record = LogRecord(
                lsn=end_lsn,
                txn_id=0,
                record_type=LogRecordType.CHECKPOINT_END,
                prev_lsn=begin_lsn,
                checkpoint_lsns=[begin_lsn, end_lsn]
            )
            self._append_log(end_record)
            self._flush_wal(end_lsn)

            self._checkpoint_lsn = end_lsn
            self._save_master_record()

            return end_lsn

    def _scan_wal_from(self, start_lsn: int, logs_by_lsn: Dict[int, LogRecord]) -> List[Tuple[int, LogRecord]]:
        """
        从指定LSN开始扫描WAL文件，加载所有日志记录

        返回按LSN排序的日志列表
        """
        if not os.path.exists(self.wal_file):
            return []

        records = []
        header_size = struct.calcsize('<IQ')
        with open(self.wal_file, 'rb') as f:
            f.seek(self.HEADER_SIZE)

            while True:
                header = f.read(header_size)
                if len(header) < header_size:
                    break
                try:
                    length, lsn = struct.unpack('<IQ', header)
                except struct.error:
                    break
                checksum = f.read(16)
                payload = f.read(length)
                if len(payload) < length:
                    break
                try:
                    record_data = header + checksum + payload
                    record = LogRecord.deserialize(record_data)
                    if record.lsn >= start_lsn:
                        logs_by_lsn[record.lsn] = record
                        records.append((record.lsn, record))
                except Exception:
                    break

        records.sort(key=lambda x: x[0])
        return records

    def _ensure_lsn_loaded(self, lsn: int, logs_by_lsn: Dict[int, LogRecord]) -> bool:
        """
        确保指定LSN的日志记录已加载到logs_by_lsn中

        如果不在，则从WAL文件头部开始扫描补充加载
        返回是否成功找到该记录
        """
        if lsn in logs_by_lsn:
            return True
        if lsn == 0:
            return False
        print(f"  [回溯加载] LSN={lsn} 不在已扫描范围内，从文件头部补充加载...")
        self._scan_wal_from(0, logs_by_lsn)
        return lsn in logs_by_lsn

    def recover(self) -> Dict[str, Any]:
        """
        崩溃恢复 (ARIES算法)

        恢复算法三阶段：

        阶段1 - 分析阶段(Analysis Pass):
        - 从最后一个有效检查点开始
        - 加载检查点保存的事务表和脏页表
        - 只从检查点之后正向扫描日志，重建状态
        - 确定重做起点(redo_lsn)和失败者事务集合

        阶段2 - 重做阶段(Redo Pass):
        - 从redo_lsn开始重放所有UPDATE/CLR日志
        - 重做所有更新(包括未提交事务的更新)
        - 检查点已刷盘的页面会被page_lsn检查自动跳过

        阶段3 - 撤销阶段(Undo Pass):
        - 找出所有未提交的失败者事务
        - 沿日志链反向遍历，遇到CLR跳到undo_next_lsn
        - 对每条UPDATE写CLR并应用before_image
        - 如果prev_lsn链回溯到检查点之前的记录，自动回溯加载
        - 最终写ROLLBACK日志

        返回恢复统计字典:
          scan_start_lsn:  本次恢复从哪个LSN开始读
          log_records_read:总共读了多少条日志(不含回溯加载)
          backtrack_loads: 撤销阶段回溯加载了多少条
          redo_count:      重做多少条
          undo_count:      撤销多少条
          checkpoint_used: 是否有效利用了检查点
        """
        print("=" * 60)
        print("开始崩溃恢复...")
        print("=" * 60)

        stats = {
            'scan_start_lsn': 0,
            'log_records_read': 0,
            'backtrack_loads': 0,
            'redo_count': 0,
            'undo_count': 0,
            'checkpoint_used': False,
            'loser_txns': [],
        }

        original_scan_start_log_size = 0
        checkpoint_redo_lsn = None

        self._load_master_record()
        print(f"主记录检查点LSN: {self._checkpoint_lsn}")

        # ========== 阶段1: 分析阶段 ==========
        print("\n[阶段1] 分析阶段 - 重建事务状态")
        print("-" * 60)

        if self._checkpoint_lsn > 0 and os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    checkpoint_data = pickle.load(f)
                for tid, entry_data in checkpoint_data.get('txn_table', {}).items():
                    self._transaction_table[tid] = TransactionTableEntry(**entry_data)
                for pid, entry_data in checkpoint_data.get('dirty_page_table', {}).items():
                    self._dirty_page_table[pid] = DirtyPageTableEntry(**entry_data)
                checkpoint_redo_lsn = checkpoint_data.get('redo_lsn')
                print(f"从检查点加载: {len(self._transaction_table)} 事务, {len(self._dirty_page_table)} 脏页, redo_lsn={checkpoint_redo_lsn}")
            except Exception as e:
                print(f"加载检查点文件失败: {e}, 从头开始恢复")
                self._transaction_table.clear()
                self._dirty_page_table.clear()
                checkpoint_redo_lsn = None

        scan_start = max(1, self._checkpoint_lsn) if self._checkpoint_lsn > 0 else 0
        stats['scan_start_lsn'] = scan_start
        logs_by_lsn: Dict[int, LogRecord] = {}
        all_records = self._scan_wal_from(scan_start, logs_by_lsn)
        scan_count_from_file = len(all_records)
        stats['log_records_read'] = scan_count_from_file
        print(f"从LSN={scan_start}扫描, 加载 {scan_count_from_file} 条日志记录")

        if self._checkpoint_lsn > 0:
            valid_checkpoint = False
            if self._checkpoint_lsn in logs_by_lsn:
                ckpt_rec = logs_by_lsn[self._checkpoint_lsn]
                if ckpt_rec.record_type == LogRecordType.CHECKPOINT_END:
                    valid_checkpoint = True
                    stats['checkpoint_used'] = True
                    print(f"找到有效检查点，LSN={self._checkpoint_lsn}")
            if not valid_checkpoint:
                print("检查点无效，从日志开头重新扫描")
                self._transaction_table.clear()
                self._dirty_page_table.clear()
                checkpoint_redo_lsn = None
                logs_by_lsn.clear()
                all_records = self._scan_wal_from(0, logs_by_lsn)
                stats['log_records_read'] = len(all_records)
                stats['scan_start_lsn'] = 0

        if self._dirty_page_table and checkpoint_redo_lsn is None:
            redo_lsn = min(e.rec_lsn for e in self._dirty_page_table.values())
        elif checkpoint_redo_lsn is not None:
            redo_lsn = checkpoint_redo_lsn
        elif self._checkpoint_lsn > 0:
            redo_lsn = self._checkpoint_lsn + 1
        else:
            redo_lsn = 1
        print(f"重做起点 LSN: {redo_lsn}")

        loser_txns = set()
        for lsn, record in all_records:
            if record.txn_id == 0:
                continue
            if record.record_type == LogRecordType.BEGIN:
                self._transaction_table[record.txn_id] = TransactionTableEntry(
                    txn_id=record.txn_id,
                    status='active',
                    last_lsn=record.lsn
                )
                loser_txns.add(record.txn_id)
            elif record.record_type == LogRecordType.UPDATE:
                if record.txn_id in self._transaction_table:
                    self._transaction_table[record.txn_id].last_lsn = record.lsn
                else:
                    self._transaction_table[record.txn_id] = TransactionTableEntry(
                        txn_id=record.txn_id, status='active', last_lsn=record.lsn
                    )
                    loser_txns.add(record.txn_id)
                if record.page_id is not None and record.page_id not in self._dirty_page_table:
                    self._dirty_page_table[record.page_id] = DirtyPageTableEntry(
                        page_id=record.page_id, rec_lsn=record.lsn
                    )
            elif record.record_type == LogRecordType.COMMIT:
                if record.txn_id in self._transaction_table:
                    self._transaction_table[record.txn_id].status = 'committed'
                    self._transaction_table[record.txn_id].last_lsn = record.lsn
                loser_txns.discard(record.txn_id)
            elif record.record_type == LogRecordType.ROLLBACK:
                if record.txn_id in self._transaction_table:
                    self._transaction_table[record.txn_id].status = 'aborted'
                    self._transaction_table[record.txn_id].last_lsn = record.lsn
                loser_txns.discard(record.txn_id)
            elif record.record_type == LogRecordType.CLR:
                if record.txn_id in self._transaction_table:
                    self._transaction_table[record.txn_id].last_lsn = record.lsn

        active_txns = {tid for tid, e in self._transaction_table.items() if e.status == 'active'}
        loser_txns = loser_txns | active_txns
        stats['loser_txns'] = sorted(loser_txns)
        print(f"分析完成: 失败者事务 = {loser_txns}")

        # ========== 阶段2: 重做阶段 ==========
        print("\n[阶段2] 重做阶段 - 重放所有更新操作")
        print("-" * 60)

        redo_count = 0
        for lsn, record in all_records:
            if lsn < redo_lsn:
                continue
            if record.record_type in (LogRecordType.UPDATE, LogRecordType.CLR):
                if record.page_id is not None and record.after_image is not None:
                    page_lsn = self.page_store.get_page_lsn(record.page_id)
                    if page_lsn < record.lsn:
                        print(f"  REDO LSN={record.lsn} 页={record.page_id} 类型={record.record_type.name}")
                        self.page_store.write_page(record.page_id, record.after_image, record.lsn)
                        redo_count += 1
                        if record.page_id not in self._dirty_page_table:
                            self._dirty_page_table[record.page_id] = DirtyPageTableEntry(
                                page_id=record.page_id, rec_lsn=record.lsn
                            )

        stats['redo_count'] = redo_count
        print(f"重做完成: 共重放 {redo_count} 条更新记录")

        # ========== 阶段3: 撤销阶段 ==========
        print("\n[阶段3] 撤销阶段 - 回滚未提交事务")
        print("-" * 60)

        loser_list = sorted(loser_txns)
        undo_count = 0
        for txn_id in loser_list:
            print(f"\n撤销事务 Txn={txn_id}")
            if txn_id in self._transaction_table:
                current_lsn = self._transaction_table[txn_id].last_lsn
            else:
                current_lsn = 0

            visited = set()
            while current_lsn != 0 and current_lsn not in visited:
                visited.add(current_lsn)

                if current_lsn not in logs_by_lsn:
                    before = len(logs_by_lsn)
                    if not self._ensure_lsn_loaded(current_lsn, logs_by_lsn):
                        print(f"  无法加载 LSN={current_lsn}, 停止撤销")
                        break
                    stats['backtrack_loads'] += len(logs_by_lsn) - before

                record = logs_by_lsn[current_lsn]

                if record.txn_id != txn_id:
                    break

                if record.record_type == LogRecordType.UPDATE:
                    if record.page_id is not None and record.before_image is not None:
                        clr_lsn = self._allocate_lsn()
                        prev_lsn = 0
                        if txn_id in self._transaction_table:
                            prev_lsn = self._transaction_table[txn_id].last_lsn

                        clr_record = LogRecord(
                            lsn=clr_lsn,
                            txn_id=txn_id,
                            record_type=LogRecordType.CLR,
                            prev_lsn=prev_lsn,
                            page_id=record.page_id,
                            before_image=record.after_image,
                            after_image=record.before_image,
                            undo_next_lsn=record.prev_lsn
                        )

                        self._append_log(clr_record)
                        print(f"  UNDO->CLR LSN={clr_lsn} 页={record.page_id} (撤销LSN={record.lsn})")

                        self.page_store.write_page(record.page_id, record.before_image, clr_lsn)
                        undo_count += 1

                        if txn_id in self._transaction_table:
                            self._transaction_table[txn_id].last_lsn = clr_lsn

                    current_lsn = record.prev_lsn

                elif record.record_type == LogRecordType.CLR:
                    next_lsn = record.undo_next_lsn if record.undo_next_lsn is not None else record.prev_lsn
                    print(f"  跳过已有CLR LSN={record.lsn} -> undo_next_lsn={next_lsn}")
                    current_lsn = next_lsn

                elif record.record_type == LogRecordType.BEGIN:
                    break

                else:
                    current_lsn = record.prev_lsn

            rollback_lsn = self._allocate_lsn()
            last_txn_lsn = self._transaction_table[txn_id].last_lsn if txn_id in self._transaction_table else 0
            rollback_record = LogRecord(
                lsn=rollback_lsn,
                txn_id=txn_id,
                record_type=LogRecordType.ROLLBACK,
                prev_lsn=last_txn_lsn
            )
            self._append_log(rollback_record)
            print(f"  写入ROLLBACK LSN={rollback_lsn}")

            if txn_id in self._transaction_table:
                self._transaction_table[txn_id].status = 'aborted'
                self._transaction_table[txn_id].last_lsn = rollback_lsn

        self._flush_wal()
        self.page_store.flush_all_pages()

        stats['undo_count'] = undo_count
        print(f"\n撤销完成: 共撤销 {undo_count} 条记录")
        print("\n" + "=" * 60)
        print("崩溃恢复完成!")
        print("-" * 60)
        print(f"  恢复统计:")
        print(f"    扫描起点LSN : {stats['scan_start_lsn']}")
        print(f"    读取日志条数 : {stats['log_records_read']}")
        print(f"    回溯加载条数 : {stats['backtrack_loads']}")
        print(f"    重做条数     : {stats['redo_count']}")
        print(f"    撤销条数     : {stats['undo_count']}")
        print(f"    检查点生效   : {'是' if stats['checkpoint_used'] else '否'}")
        print("=" * 60)

        return stats

    def get_stats(self) -> Dict[str, Any]:
        """获取WAL状态统计"""
        return {
            'current_lsn': self._current_lsn,
            'flushed_lsn': self._flushed_lsn,
            'checkpoint_lsn': self._checkpoint_lsn,
            'active_txns': len([e for e in self._transaction_table.values() if e.status == 'active']),
            'committed_txns': len([e for e in self._transaction_table.values() if e.status == 'committed']),
            'dirty_pages': len(self._dirty_page_table),
            'total_txns': len(self._transaction_table),
        }

    def truncate_wal(self) -> Dict[str, Any]:
        """
        安全清理日志: 删除最后一个检查点之前的所有日志记录。

        安全保证:
        1. 必须有有效的检查点 (checkpoint_lsn > 0)
        2. 确保检查点前所有脏页都已刷盘 (dirty_page_table必须为空或全部rec_lsn >= checkpoint_lsn)
        3. 确保没有活跃事务的日志链起点早于检查点 (即检查点文件的txn_table中所有事务都已提交/回滚)

        清理策略:
        - 不会物理删除WAL文件(LSN是顺序递增、文件内偏移索引会乱), 而是:
          a. 把 [checkpoint_begin_lsn, current_lsn] 这一段日志拷贝出来
          b. 用原子替换把WAL文件换成新内容
          c. 保留的最小编号是 checkpoint_begin_lsn, 文件前4096字节头部不动,
             低于 checkpoint_begin_lsn 的记录全部被丢弃
        """
        with self._lock:
            stats = {
                'old_log_records': 0,
                'new_log_records': 0,
                'old_size_bytes': 0,
                'new_size_bytes': 0,
                'error': None,
                'truncated': False,
                'min_retained_lsn': 0,
            }

            if self._checkpoint_lsn == 0:
                stats['error'] = '没有检查点, 无法清理'
                return stats

            if not os.path.exists(self.checkpoint_file):
                stats['error'] = '检查点文件不存在'
                return stats

            with open(self.checkpoint_file, 'rb') as f:
                cp_data = pickle.load(f)

            cp_begin_lsn = cp_data.get('begin_lsn')
            if cp_begin_lsn is None:
                stats['error'] = '旧检查点格式, 不含begin_lsn, 请先创建新检查点'
                return stats

            cp_dirty = cp_data.get('dirty_page_table', {})
            if cp_dirty:
                stats['error'] = f'检查点记录了 {len(cp_dirty)} 个脏页, 清理不安全'
                return stats

            cp_txns = cp_data.get('txn_table', {})
            for tid, entry in cp_txns.items():
                status = entry.get('status') if isinstance(entry, dict) else getattr(entry, 'status', None)
                if status == 'active':
                    stats['error'] = f'检查点中有活跃事务 Txn={tid}, 它的撤销链可能回落到清理范围外, 无法安全清理'
                    return stats

            self._flush_wal(self._current_lsn)

            logs_by_lsn: Dict[int, LogRecord] = {}
            all_records = self._scan_wal_from(0, logs_by_lsn)
            stats['old_log_records'] = len(all_records)
            stats['old_size_bytes'] = os.path.getsize(self.wal_file) if os.path.exists(self.wal_file) else 0

            retained = [r for _, r in all_records if r.lsn >= cp_begin_lsn]
            retained.sort(key=lambda r: r.lsn)
            stats['new_log_records'] = len(retained)
            stats['min_retained_lsn'] = cp_begin_lsn

            tmp_wal = self.wal_file + '.tmp.trunc'
            with open(tmp_wal, 'wb') as f:
                f.write(b'\x00' * self.HEADER_SIZE)
                for r in retained:
                    data = r.serialize()
                    f.write(data)

            os.replace(tmp_wal, self.wal_file)
            stats['new_size_bytes'] = os.path.getsize(self.wal_file)
            stats['truncated'] = True

            return stats


class SimulatedCrash(Exception):
    """模拟崩溃的异常 - 用来在关键节点中断流程"""
    def __init__(self, point: str, info: str = ''):
        self.point = point
        self.info = info
        super().__init__(f'Crash injected at: {point} {info}')


class CrashInjector:
    """
    WAL崩溃注入工具

    支持的注入点 (inject_point):
      - 'after_update_log'    : 更新操作写完日志、还没写数据页时崩溃
      - 'after_update_page'   : 更新操作写完日志、也写完数据页后崩溃
      - 'after_commit_log'    : 提交事务写完COMMIT日志后崩溃
      - 'after_clr_log'       : 回滚过程中写完一条CLR日志、但还没写数据页时崩溃
      - 'after_clr_page'      : 回滚过程中写完CLR日志、也写完数据页后崩溃 (即某条记录已撤销完成)
      - 'after_rollback_log'  : 回滚完成、写完ROLLBACK日志后崩溃
      - 'after_checkpoint_begin' : 检查点写完BEGIN日志后崩溃
      - 'after_checkpoint_flush' : 检查点刷完脏页后崩溃
      - 'after_checkpoint_end'   : 检查点写完END日志、但还没更新master record时崩溃

    使用方式:
      injector = CrashInjector(wal, 'after_update_page', after_n=2)
      injector.attach()
      try:
          run_workload()
      except SimulatedCrash as e:
          print(f'崩溃于: {e.point}')
      injector.detach()
    """

    VALID_POINTS = {
        'after_update_log', 'after_update_page',
        'after_commit_log',
        'after_clr_log', 'after_clr_page',
        'after_rollback_log',
        'after_checkpoint_begin', 'after_checkpoint_flush', 'after_checkpoint_end',
    }

    def __init__(self, wal: 'WALManager', inject_point: str, after_n: int = 1):
        assert inject_point in self.VALID_POINTS, f'未知注入点: {inject_point}'
        self.wal = wal
        self.inject_point = inject_point
        self.after_n = after_n
        self._counter = 0
        self._attached = False
        self._orig_update = None
        self._orig_commit = None
        self._orig_undo = None
        self._orig_undo_txn = None
        self._orig_create_ckpt = None
        self.last_fired_lsn = 0

    def attach(self):
        if self._attached:
            return
        self._attached = True
        self._counter = 0

        wal = self.wal

        if self.inject_point in ('after_update_log', 'after_update_page'):
            self._orig_update = wal.update
            def patched_update(txn_id, page_id, before_image, after_image):
                with wal._lock:
                    if txn_id not in wal._transaction_table:
                        raise ValueError(f"事务 {txn_id} 不存在或已结束")
                    txn_entry = wal._transaction_table[txn_id]
                    if txn_entry.status != 'active':
                        raise ValueError(f"事务 {txn_id} 状态不是活跃")
                    lsn = wal._allocate_lsn()
                    prev_lsn = wal._txn_prev_lsn[txn_id]
                    record = LogRecord(
                        lsn=lsn, txn_id=txn_id,
                        record_type=LogRecordType.UPDATE,
                        prev_lsn=prev_lsn, page_id=page_id,
                        before_image=copy.deepcopy(before_image),
                        after_image=copy.deepcopy(after_image)
                    )
                    wal._append_log(record)
                    wal._flush_wal(lsn)
                    if self.inject_point == 'after_update_log':
                        self._maybe_crash(lsn, f'page={page_id}')
                    wal.page_store.write_page(page_id, after_image, lsn)
                    if page_id not in wal._dirty_page_table:
                        wal._dirty_page_table[page_id] = DirtyPageTableEntry(
                            page_id=page_id, rec_lsn=lsn)
                    txn_entry.last_lsn = lsn
                    wal._txn_prev_lsn[txn_id] = lsn
                    if self.inject_point == 'after_update_page':
                        self._maybe_crash(lsn, f'page={page_id}')
                    return lsn
            wal.update = patched_update

        if self.inject_point == 'after_commit_log':
            self._orig_commit = wal.commit_transaction
            def patched_commit(txn_id):
                with wal._lock:
                    if txn_id not in wal._transaction_table:
                        raise ValueError(f"事务 {txn_id} 不存在")
                    txn_entry = wal._transaction_table[txn_id]
                    if txn_entry.status != 'active':
                        return
                    lsn = wal._allocate_lsn()
                    prev_lsn = wal._txn_prev_lsn[txn_id]
                    commit_record = LogRecord(
                        lsn=lsn, txn_id=txn_id,
                        record_type=LogRecordType.COMMIT,
                        prev_lsn=prev_lsn
                    )
                    wal._append_log(commit_record)
                    wal._flush_wal(lsn)
                    self._maybe_crash(lsn, f'txn={txn_id}')
                    txn_entry.status = 'committed'
                    txn_entry.last_lsn = lsn
                    wal._txn_prev_lsn[txn_id] = lsn
                    wal._save_master_record()
            wal.commit_transaction = patched_commit

        if self.inject_point in ('after_clr_log', 'after_clr_page', 'after_rollback_log'):
            self._orig_undo_txn = wal._undo_transaction
            def patched_undo(txn_id):
                logs_by_lsn: Dict[int, LogRecord] = {}
                wal._scan_wal_from(0, logs_by_lsn)
                if txn_id in wal._transaction_table:
                    current_lsn = wal._transaction_table[txn_id].last_lsn
                else:
                    current_lsn = wal._txn_prev_lsn.get(txn_id, 0)
                visited = set()
                rollback_written = False
                while current_lsn != 0 and current_lsn not in visited:
                    visited.add(current_lsn)
                    if current_lsn not in logs_by_lsn:
                        break
                    record = logs_by_lsn[current_lsn]
                    if record.txn_id != txn_id:
                        break
                    if record.record_type == LogRecordType.UPDATE:
                        if record.page_id is not None and record.before_image is not None:
                            clr_lsn = wal._allocate_lsn()
                            prev_lsn = wal._txn_prev_lsn.get(txn_id, 0)
                            clr_record = LogRecord(
                                lsn=clr_lsn, txn_id=txn_id,
                                record_type=LogRecordType.CLR,
                                prev_lsn=prev_lsn, page_id=record.page_id,
                                before_image=record.after_image,
                                after_image=record.before_image,
                                undo_next_lsn=record.prev_lsn
                            )
                            wal._append_log(clr_record)
                            if self.inject_point == 'after_clr_log':
                                self._maybe_crash(clr_lsn, f'page={record.page_id} undo_of_lsn={record.lsn}')
                            wal.page_store.write_page(record.page_id, record.before_image, clr_lsn)
                            wal._txn_prev_lsn[txn_id] = clr_lsn
                            if self.inject_point == 'after_clr_page':
                                self._maybe_crash(clr_lsn, f'page={record.page_id} undo_of_lsn={record.lsn}')
                        current_lsn = record.prev_lsn
                    elif record.record_type == LogRecordType.CLR:
                        current_lsn = (record.undo_next_lsn
                                       if record.undo_next_lsn is not None else record.prev_lsn)
                    elif record.record_type == LogRecordType.BEGIN:
                        break
                    else:
                        current_lsn = record.prev_lsn
                rollback_lsn = wal._allocate_lsn()
                last_txn_lsn = (wal._transaction_table[txn_id].last_lsn
                                if txn_id in wal._transaction_table else 0)
                rollback_record = LogRecord(
                    lsn=rollback_lsn, txn_id=txn_id,
                    record_type=LogRecordType.ROLLBACK,
                    prev_lsn=last_txn_lsn
                )
                wal._append_log(rollback_record)
                if self.inject_point == 'after_rollback_log':
                    self._maybe_crash(rollback_lsn, f'txn={txn_id}')
                wal._flush_wal(rollback_lsn)
                if txn_id in wal._transaction_table:
                    wal._transaction_table[txn_id].status = 'aborted'
                    wal._transaction_table[txn_id].last_lsn = rollback_lsn
                return rollback_written
            wal._undo_transaction = patched_undo

            self._orig_abort = getattr(wal, 'abort_transaction', None)
            def patched_abort(txn_id):
                with wal._lock:
                    if txn_id not in wal._transaction_table:
                        raise ValueError(f"事务 {txn_id} 不存在")
                    wal._undo_transaction(txn_id)
                    wal._flush_wal()
            wal.abort_transaction = patched_abort

        if self.inject_point in ('after_checkpoint_begin', 'after_checkpoint_flush', 'after_checkpoint_end'):
            self._orig_create_ckpt = wal.create_checkpoint
            def patched_ckpt():
                with wal._lock:
                    begin_lsn = wal._allocate_lsn()
                    begin_record = LogRecord(
                        lsn=begin_lsn, txn_id=0,
                        record_type=LogRecordType.CHECKPOINT_BEGIN,
                        prev_lsn=0
                    )
                    wal._append_log(begin_record)
                    wal._flush_wal(begin_lsn)
                    if self.inject_point == 'after_checkpoint_begin':
                        self._maybe_crash(begin_lsn, 'checkpoint_begin')
                    checkpoint_txn_table = copy.deepcopy(wal._transaction_table)
                    dirty_pages = list(wal._dirty_page_table.keys())
                    for page_id in dirty_pages:
                        wal.flush_pages_with_wal_guarantee(page_id)
                    checkpoint_dirty_table = copy.deepcopy(wal._dirty_page_table)
                    if checkpoint_dirty_table:
                        redo_lsn = min(e.rec_lsn for e in checkpoint_dirty_table.values())
                    else:
                        redo_lsn = begin_lsn + 1
                    checkpoint_data = {
                        'txn_table': {tid: e.__dict__ for tid, e in checkpoint_txn_table.items()},
                        'dirty_page_table': {pid: e.__dict__ for pid, e in checkpoint_dirty_table.items()},
                        'redo_lsn': redo_lsn, 'begin_lsn': begin_lsn,
                    }
                    tmp_file = wal.checkpoint_file + '.tmp'
                    with open(tmp_file, 'wb') as f:
                        pickle.dump(checkpoint_data, f)
                    os.replace(tmp_file, wal.checkpoint_file)
                    if self.inject_point == 'after_checkpoint_flush':
                        self._maybe_crash(begin_lsn, 'checkpoint_flush_done')
                    end_lsn = wal._allocate_lsn()
                    end_record = LogRecord(
                        lsn=end_lsn, txn_id=0,
                        record_type=LogRecordType.CHECKPOINT_END,
                        prev_lsn=begin_lsn,
                        checkpoint_lsns=[begin_lsn, end_lsn]
                    )
                    wal._append_log(end_record)
                    wal._flush_wal(end_lsn)
                    if self.inject_point == 'after_checkpoint_end':
                        self._maybe_crash(end_lsn, 'checkpoint_end_written')
                    wal._checkpoint_lsn = end_lsn
                    wal._save_master_record()
                    return end_lsn
            wal.create_checkpoint = patched_ckpt

    def _maybe_crash(self, lsn: int, info: str):
        self._counter += 1
        if self._counter >= self.after_n:
            self.wal._flush_wal(lsn)
            self.wal.page_store.flush_all_pages()
            self.last_fired_lsn = lsn
            raise SimulatedCrash(self.inject_point, f'LSN={lsn} {info}')

    def detach(self):
        if not self._attached:
            return
        self._attached = False
        wal = self.wal
        if self._orig_update is not None:
            wal.update = self._orig_update
        if self._orig_commit is not None:
            wal.commit_transaction = self._orig_commit
        if self._orig_undo_txn is not None:
            wal._undo_transaction = self._orig_undo_txn
            if hasattr(self, '_orig_abort') and self._orig_abort is not None:
                wal.abort_transaction = self._orig_abort
        if self._orig_create_ckpt is not None:
            wal.create_checkpoint = self._orig_create_ckpt
