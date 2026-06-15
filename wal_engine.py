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
        执行事务的UNDO操作

        通过反向遍历日志链进行撤销

        关键：使用prev_lsn链反向遍历同一事务的日志
        """
        logs_by_lsn: Dict[int, LogRecord] = {}
        self._scan_wal_from(self._checkpoint_lsn, logs_by_lsn)

        undo_stack = []
        if txn_id in self._transaction_table:
            last_lsn = self._transaction_table[txn_id].last_lsn
        else:
            last_lsn = self._txn_prev_lsn.get(txn_id, 0)

        current_lsn = last_lsn
        while current_lsn != 0 and current_lsn in logs_by_lsn:
            record = logs_by_lsn[current_lsn]
            if record.txn_id == txn_id:
                if record.record_type == LogRecordType.UPDATE:
                    undo_stack.append(record)
                current_lsn = record.prev_lsn
            else:
                break

        for record in reversed(undo_stack):
            if record.record_type == LogRecordType.UPDATE and record.page_id is not None:
                clr_lsn = self._allocate_lsn()
                prev_lsn = self._txn_prev_lsn.get(txn_id, 0)

                clr_record = LogRecord(
                    lsn=clr_lsn,
                    txn_id=txn_id,
                    record_type=LogRecordType.CLR,
                    prev_lsn=prev_lsn,
                    page_id=record.page_id,
                    before_image=record.before_image,
                    after_image=record.after_image
                )

                self._append_log(clr_record)

                if record.before_image is not None:
                    self.page_store.write_page(
                        record.page_id,
                        record.before_image,
                        clr_lsn
                    )

                self._txn_prev_lsn[txn_id] = clr_lsn

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
        3. 保存检查点信息（事务表+脏页表）
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
            checkpoint_dirty_table = copy.deepcopy(self._dirty_page_table)

            dirty_pages = list(self._dirty_page_table.keys())
            for page_id in dirty_pages:
                self.flush_pages_with_wal_guarantee(page_id)

            checkpoint_data = {
                'txn_table': {tid: e.__dict__ for tid, e in checkpoint_txn_table.items()},
                'dirty_page_table': {pid: e.__dict__ for pid, e in checkpoint_dirty_table.items()},
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

    def recover(self):
        """
        崩溃恢复

        恢复算法三阶段：

        阶段1 - 分析阶段(Analysis Pass):
        - 找到最后一个有效检查点
        - 重建事务表和脏页表
        - 确定重做起点(redo_lsn)

        阶段2 - 重做阶段(Redo Pass):
        - 从redo_lsn开始重放所有日志
        - 重做所有更新(包括未提交事务的更新)
        - 保证已提交和未提交的修改都重做

        阶段3 - 撤销阶段(Undo Pass):
        - 找出所有未提交的失败者事务
        - 反向遍历UNDO每个失败者事务
        - 使用before_image恢复数据
        - 写入CLR和ROLLBACK日志
        """
        print("=" * 60)
        print("开始崩溃恢复...")
        print("=" * 60)

        self._load_master_record()
        print(f"主记录检查点LSN: {self._checkpoint_lsn}")

        # ========== 阶段1: 分析阶段 ==========
        print("\n[阶段1] 分析阶段 - 重建事务状态")
        print("-" * 60)

        logs_by_lsn: Dict[int, LogRecord] = {}
        all_records = self._scan_wal_from(0, logs_by_lsn)

        if self._checkpoint_lsn > 0 and self._checkpoint_lsn in logs_by_lsn:
            checkpoint_record = logs_by_lsn[self._checkpoint_lsn]
            if checkpoint_record.record_type == LogRecordType.CHECKPOINT_END:
                print(f"找到有效检查点，LSN={self._checkpoint_lsn}")

                if os.path.exists(self.checkpoint_file):
                    try:
                        with open(self.checkpoint_file, 'rb') as f:
                            checkpoint_data = pickle.load(f)
                        for tid, entry_data in checkpoint_data.get('txn_table', {}).items():
                            self._transaction_table[tid] = TransactionTableEntry(**entry_data)
                        for pid, entry_data in checkpoint_data.get('dirty_page_table', {}).items():
                            self._dirty_page_table[pid] = DirtyPageTableEntry(**entry_data)
                        print(f"从检查点加载: {len(self._transaction_table)} 事务, {len(self._dirty_page_table)} 脏页")
                    except Exception as e:
                        print(f"加载检查点文件失败: {e}, 从头开始恢复")

        redo_lsn = min([e.rec_lsn for e in self._dirty_page_table.values()], default=self._checkpoint_lsn + 1)
        if self._checkpoint_lsn == 0:
            redo_lsn = 1
        print(f"重做起点 LSN: {redo_lsn}")

        loser_txns = set()
        for lsn, record in all_records:
            if record.txn_id == 0:
                continue
            if record.record_type == LogRecordType.BEGIN:
                if record.txn_id not in self._transaction_table:
                    self._transaction_table[record.txn_id] = TransactionTableEntry(
                        txn_id=record.txn_id,
                        status='active',
                        last_lsn=record.lsn
                    )
                    loser_txns.add(record.txn_id)
            elif record.record_type == LogRecordType.UPDATE:
                if record.txn_id in self._transaction_table:
                    self._transaction_table[record.txn_id].last_lsn = record.lsn
                if record.page_id is not None:
                    if record.page_id not in self._dirty_page_table:
                        self._dirty_page_table[record.page_id] = DirtyPageTableEntry(
                            page_id=record.page_id,
                            rec_lsn=record.lsn
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
                    page = self.page_store.read_page(record.page_id)
                    page_lsn = self.page_store.get_page_lsn(record.page_id)
                    if page_lsn < record.lsn:
                        print(f"  REDO LSN={record.lsn} 页={record.page_id}")
                        self.page_store.write_page(record.page_id, record.after_image, record.lsn)
                        redo_count += 1
                        if record.page_id not in self._dirty_page_table:
                            self._dirty_page_table[record.page_id] = DirtyPageTableEntry(
                                page_id=record.page_id,
                                rec_lsn=record.lsn
                            )

        print(f"重做完成: 共重放 {redo_count} 条更新记录")

        # ========== 阶段3: 撤销阶段 ==========
        print("\n[阶段3] 撤销阶段 - 回滚未提交事务")
        print("-" * 60)

        loser_list = sorted(loser_txns)
        undo_count = 0
        for txn_id in loser_list:
            print(f"\n撤销事务 Txn={txn_id}")
            undo_stack = []
            if txn_id in self._transaction_table:
                last_lsn = self._transaction_table[txn_id].last_lsn
            else:
                last_lsn = 0
                for lsn, rec in reversed(all_records):
                    if rec.txn_id == txn_id and rec.record_type != LogRecordType.BEGIN:
                        last_lsn = max(last_lsn, lsn)

            current_lsn = last_lsn
            visited = set()
            while current_lsn != 0 and current_lsn not in visited:
                visited.add(current_lsn)
                if current_lsn in logs_by_lsn:
                    record = logs_by_lsn[current_lsn]
                    if record.txn_id == txn_id:
                        if record.record_type in (LogRecordType.UPDATE,):
                            undo_stack.append(record)
                        current_lsn = record.prev_lsn
                    else:
                        break
                else:
                    break

            for record in undo_stack:
                if record.record_type == LogRecordType.UPDATE and record.page_id is not None and record.before_image is not None:
                    print(f"  UNDO LSN={record.lsn} 页={record.page_id}")
                    self.page_store.write_page(record.page_id, record.before_image, 0)
                    undo_count += 1

            rollback_lsn = self._allocate_lsn()
            rollback_record = LogRecord(
                lsn=rollback_lsn,
                txn_id=txn_id,
                record_type=LogRecordType.ROLLBACK,
                prev_lsn=last_lsn
            )
            self._append_log(rollback_record)

            if txn_id in self._transaction_table:
                self._transaction_table[txn_id].status = 'aborted'
                self._transaction_table[txn_id].last_lsn = rollback_lsn

        self._flush_wal()
        self.page_store.flush_all_pages()

        print(f"\n撤销完成: 共撤销 {undo_count} 条记录")
        print("\n" + "=" * 60)
        print("崩溃恢复完成!")
        print("=" * 60)

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
