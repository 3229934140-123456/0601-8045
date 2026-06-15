"""
WAL模块验收测试 - 针对用户提出的4个场景

1. 同一事务连续改同一页再回滚 → 应恢复到事务前原始值
2. 事务未提交时做检查点再回滚 → 检查点前后改过的数据都撤掉
3. 检查点缩短恢复范围 → 不从第一条日志开始扫
4. 回滚中途崩溃 → 重启恢复后数据保持已回滚的结果
"""

import os
import shutil
import sys
from wal_engine import WALManager, LogRecordType, LogRecord

TEST_WAL_DIR = 'test_wal'
TEST_DATA_DIR = 'test_data'


def clean_up():
    for d in [TEST_WAL_DIR, TEST_DATA_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)


def sep(title=''):
    print('\n' + '=' * 70)
    if title:
        print(f'  {title}')
        print('=' * 70)


# ============================================================
# 测试1: 同一事务连续改同一页再回滚
# 余额 100 → 200 → 300, 回滚后应该回到 100
# ============================================================
def test_1_same_page_multiple_updates_rollback():
    sep('测试1: 同一事务连续改同一页再回滚')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    txn = wal.begin_transaction()
    print(f'开始事务 Txn={txn}')

    page_before_1 = {'id': 1, 'data': {'balance': 100}}
    page_after_1 = {'id': 1, 'data': {'balance': 200}}
    wal.update(txn, 1, page_before_1, page_after_1)
    print(f'  页1: balance 100 → 200')

    page_before_2 = {'id': 1, 'data': {'balance': 200}}
    page_after_2 = {'id': 1, 'data': {'balance': 300}}
    wal.update(txn, 1, page_before_2, page_after_2)
    print(f'  页1: balance 200 → 300')

    wal.abort_transaction(txn)
    print(f'回滚事务')

    page = wal.page_store.read_page(1)
    print(f'\n回滚后页1 balance = {page["data"]["balance"]}')

    assert page['data']['balance'] == 100, f'期望100, 实际{page["data"]["balance"]}'

    print('\n[PASS] 测试1通过: 连续改同一页回滚后恢复到原始值100')
    clean_up()


# ============================================================
# 测试2: 事务未提交时做检查点再回滚
# 检查点前改页1, 检查点后再改页2, 回滚后两页都回原值
# ============================================================
def test_2_checkpoint_then_rollback():
    sep('测试2: 事务未提交时做检查点再回滚')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    txn = wal.begin_transaction()
    print(f'开始事务 Txn={txn}')

    page1_before = {'id': 1, 'data': {'val': 'A'}}
    page1_after = {'id': 1, 'data': {'val': 'B'}}
    wal.update(txn, 1, page1_before, page1_after)
    print(f'  检查点前: 页1 val A → B')

    ckpt = wal.create_checkpoint()
    print(f'  创建检查点 LSN={ckpt}')

    page2_before = {'id': 2, 'data': {'val': 'X'}}
    page2_after = {'id': 2, 'data': {'val': 'Y'}}
    wal.update(txn, 2, page2_before, page2_after)
    print(f'  检查点后: 页2 val X → Y')

    wal.abort_transaction(txn)
    print(f'回滚事务')

    page1 = wal.page_store.read_page(1)
    page2 = wal.page_store.read_page(2)
    print(f'\n回滚后 页1 val = {page1["data"]["val"]}')
    print(f'回滚后 页2 val = {page2["data"]["val"]}')

    assert page1['data']['val'] == 'A', f'页1期望A, 实际{page1["data"]["val"]}'
    assert page2['data']['val'] == 'X', f'页2期望X, 实际{page2["data"]["val"]}'

    print('\n[PASS] 测试2通过: 检查点前后改过的数据都回滚成功')
    clean_up()


# ============================================================
# 测试3: 检查点真的能缩短恢复范围
# 造一批检查点前的历史事务, 再在检查点后写少量日志
# 重启恢复时只处理检查点之后需要处理的部分
# ============================================================
def test_3_checkpoint_shortens_recovery():
    sep('测试3: 检查点缩短恢复范围')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    history_count = 50
    print(f'创建 {history_count} 个历史事务(检查点前)...')
    for i in range(history_count):
        txn = wal.begin_transaction()
        page_before = {'id': i + 1, 'data': {'v': i * 10}}
        page_after = {'id': i + 1, 'data': {'v': i * 10 + 1}}
        wal.update(txn, i + 1, page_before, page_after)
        wal.commit_transaction(txn)

    lsn_before_ckpt = wal._current_lsn
    print(f'检查点前最后LSN: {lsn_before_ckpt}')

    ckpt = wal.create_checkpoint()
    print(f'创建检查点 LSN={ckpt}')

    txn_active = wal.begin_transaction()
    page_before = {'id': 100, 'data': {'v': 0}}
    page_after = {'id': 100, 'data': {'v': 999}}
    wal.update(txn_active, 100, page_before, page_after)
    print(f'检查点后: 开始未提交事务 Txn={txn_active}, 页100 v=0→999')

    txn_committed = wal.begin_transaction()
    page_before_c = {'id': 101, 'data': {'v': 0}}
    page_after_c = {'id': 101, 'data': {'v': 555}}
    wal.update(txn_committed, 101, page_before_c, page_after_c)
    wal.commit_transaction(txn_committed)
    print(f'检查点后: 提交事务 Txn={txn_committed}, 页101 v=0→555')

    lsn_after = wal._current_lsn
    print(f'检查点后最后LSN: {lsn_after}')

    print('\n模拟崩溃...')
    del wal

    print('\n重启系统，执行恢复...')
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    import io
    from contextlib import redirect_stdout

    capture = io.StringIO()
    with redirect_stdout(capture):
        wal2.recover()
    output = capture.getvalue()
    print(output)

    lines = output.split('\n')
    redo_lines = [l for l in lines if 'REDO' in l]
    print(f'\nREDO操作数: {len(redo_lines)} (应远小于历史事务数{history_count})')
    print(f'检查点前LSN范围: 1~{lsn_before_ckpt}, 检查点后LSN范围: {ckpt}~{lsn_after}')

    for line in redo_lines:
        print(f'  {line.strip()}')

    page100 = wal2.page_store.read_page(100)
    page101 = wal2.page_store.read_page(101)
    print(f'\n页100 v = {page100["data"]["v"]} (未提交事务, 应被撤销回0)')
    print(f'页101 v = {page101["data"]["v"]} (已提交事务, 应保持555)')

    assert page100['data']['v'] == 0, f'页100期望0, 实际{page100["data"]["v"]}'
    assert page101['data']['v'] == 555, f'页101期望555, 实际{page101["data"]["v"]}'

    assert len(redo_lines) < history_count, f'REDO数{len(redo_lines)}应小于历史事务数{history_count}'

    print(f'\n[PASS] 测试3通过: 检查点缩短恢复范围, REDO只处理 {len(redo_lines)} 条 << 历史 {history_count} 条')
    clean_up()


# ============================================================
# 测试4: 回滚中途崩溃 → 重启恢复后数据保持已回滚的结果
#
# 场景: 事务改了页1和页2, 主动回滚时先撤销页2(写CLR),
#       然后模拟崩溃(不写ROLLBACK, 不撤销页1)
#       重启恢复后: 页2应保持已撤销的结果, 页1也应被继续撤销
# ============================================================
def test_4_crash_during_rollback():
    sep('测试4: 回滚中途崩溃, 恢复后数据保持已回滚的结果')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    wal.page_store.write_page(1, {'id': 1, 'data': {'name': 'original_1', 'score': 50}}, 0)
    wal.page_store.write_page(2, {'id': 2, 'data': {'name': 'original_2', 'score': 60}}, 0)
    wal.page_store.flush_all_pages()
    print('初始化: 页1={original_1, 50}, 页2={original_2, 60}')

    txn = wal.begin_transaction()
    print(f'开始事务 Txn={txn}')

    page1_before = {'id': 1, 'data': {'name': 'original_1', 'score': 50}}
    page1_after = {'id': 1, 'data': {'name': 'modified_1', 'score': 99}}
    wal.update(txn, 1, page1_before, page1_after)
    print(f'  更新页1: score 50 → 99')

    page2_before = {'id': 2, 'data': {'name': 'original_2', 'score': 60}}
    page2_after = {'id': 2, 'data': {'name': 'modified_2', 'score': 88}}
    wal.update(txn, 2, page2_before, page2_after)
    print(f'  更新页2: score 60 → 88')

    print('\n--- 模拟回滚中途崩溃 ---')
    print('手动撤销页2(写CLR), 但不撤销页1, 也不写ROLLBACK...')

    last_lsn = wal._transaction_table[txn].last_lsn
    prev_lsn = wal._txn_prev_lsn[txn]

    clr_lsn = wal._allocate_lsn()
    clr_record = LogRecord(
        lsn=clr_lsn,
        txn_id=txn,
        record_type=LogRecordType.CLR,
        prev_lsn=prev_lsn,
        page_id=2,
        before_image=page2_before,
        after_image=page2_after,
        undo_next_lsn=wal._transaction_table[txn].last_lsn
    )
    wal._append_log(clr_record)
    wal._flush_wal(clr_lsn)

    wal.page_store.write_page(2, page2_before, clr_lsn)
    wal._transaction_table[txn].last_lsn = clr_lsn
    wal._txn_prev_lsn[txn] = clr_lsn

    print(f'  写入CLR LSN={clr_lsn}, 撤销页2 → score回到60')
    print(f'  页1仍然是score=99 (未撤销)')
    print(f'  没有写ROLLBACK日志 → 模拟崩溃!')

    del wal

    print('\n重启系统，执行恢复...')
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    wal2.recover()

    page1 = wal2.page_store.read_page(1)
    page2 = wal2.page_store.read_page(2)
    print(f'\n恢复后 页1: {page1["data"]}')
    print(f'恢复后 页2: {page2["data"]}')

    assert page1['data']['score'] == 50, f'页1 score期望50, 实际{page1["data"]["score"]}'
    assert page1['data']['name'] == 'original_1', f'页1 name期望original_1, 实际{page1["data"]["name"]}'
    assert page2['data']['score'] == 60, f'页2 score期望60, 实际{page2["data"]["score"]}'
    assert page2['data']['name'] == 'original_2', f'页2 name期望original_2, 实际{page2["data"]["name"]}'

    print('\n[PASS] 测试4通过: 回滚中途崩溃后恢复, 已撤销的保持, 未撤销的继续撤销')
    clean_up()


# ============================================================
# 运行全部验收测试
# ============================================================
def run_all():
    print('\n' + '*' * 70)
    print('*' + ' ' * 15 + 'WAL模块用户验收测试 (4个场景)' + ' ' * 19 + '*')
    print('*' * 70)

    tests = [
        ('同一页多次修改回滚', test_1_same_page_multiple_updates_rollback),
        ('检查点+回滚', test_2_checkpoint_then_rollback),
        ('检查点缩短恢复范围', test_3_checkpoint_shortens_recovery),
        ('回滚中途崩溃恢复', test_4_crash_during_rollback),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f'\n[FAIL] "{name}" 失败: {e}')
            import traceback
            traceback.print_exc()
            failed += 1
            clean_up()

    sep('验收总结')
    print(f'通过: {passed}/{len(tests)}')
    if failed == 0:
        print('\n  ✅ 全部验收通过!')
    else:
        print(f'\n  ❌ {failed} 个验收失败')

    clean_up()
    return failed == 0


if __name__ == '__main__':
    success = run_all()
    sys.exit(0 if success else 1)
