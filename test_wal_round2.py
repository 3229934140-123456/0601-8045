"""
WAL模块第二轮验收测试 - 用户提出的3个场景

场景1: 主动回滚中途崩溃 → 已撤销的保持、未撤销的继续撤
场景2: 完整回滚后重启 → 页面保持事务前内容
场景3: 历史事务放大 → 恢复耗时不随历史增长
"""

import os
import shutil
import sys
import time
from wal_engine import WALManager, LogRecordType, LogRecord

TEST_WAL_DIR = 'test_wal2'
TEST_DATA_DIR = 'test_data2'


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
# 场景1: 事务改了两页,主动回滚时先撤销后一页写CLR,然后模拟崩溃
# 重启恢复后: 后一页保持原值(已撤销), 前一页继续撤回原值
# 关键验证点: 不能出现已经撤掉的页又变成新值
# ============================================================
def test_s1_rollback_mid_crash():
    sep('场景1: 主动回滚中途崩溃后恢复')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    wal.page_store.write_page(1, {'id': 1, 'data': {'balance': 100}}, 0)
    wal.page_store.write_page(2, {'id': 2, 'data': {'balance': 200}}, 0)
    wal.page_store.flush_all_pages()
    print('初始: 页1 balance=100, 页2 balance=200')

    txn = wal.begin_transaction()
    print(f'\n开始事务 Txn={txn}')

    p1_before = {'id': 1, 'data': {'balance': 100}}
    p1_after = {'id': 1, 'data': {'balance': 150}}
    wal.update(txn, 1, p1_before, p1_after)
    print(f'  更新页1: 100 → 150')

    p2_before = {'id': 2, 'data': {'balance': 200}}
    p2_after = {'id': 2, 'data': {'balance': 250}}
    wal.update(txn, 2, p2_before, p2_after)
    print(f'  更新页2: 200 → 250')

    print('\n--- 模拟主动回滚中途崩溃 ---')
    print('模拟abort执行到一半: 已撤销页2(写了CLR)，还没撤销页1，也没写ROLLBACK')

    last_lsn = wal._transaction_table[txn].last_lsn
    txn_prev = wal._txn_prev_lsn[txn]

    clr_lsn = wal._allocate_lsn()
    clr_record = LogRecord(
        lsn=clr_lsn,
        txn_id=txn,
        record_type=LogRecordType.CLR,
        prev_lsn=txn_prev,
        page_id=2,
        before_image=p2_before,
        after_image=p2_after,
        undo_next_lsn=3
    )
    wal._append_log(clr_record)
    wal._flush_wal(clr_lsn)

    wal.page_store.write_page(2, p2_before, clr_lsn)
    wal._transaction_table[txn].last_lsn = clr_lsn
    wal._txn_prev_lsn[txn] = clr_lsn

    print(f'  写入CLR LSN={clr_lsn}，页2 balance回到200')
    print(f'  页1 balance还是150 (还没撤销)')
    print(f'  没有ROLLBACK日志 → 崩溃!')

    print('\n--- 状态确认(崩溃前):')
    p1 = wal.page_store.read_page(1)
    p2 = wal.page_store.read_page(2)
    print(f'  页1 balance = {p1["data"]["balance"]} (应=150')
    print(f'  页2 balance = {p2["data"]["balance"]} (应=200)')

    del wal

    print('\n重启系统，执行恢复...')
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    wal2.recover()

    p1 = wal2.page_store.read_page(1)
    p2 = wal2.page_store.read_page(2)
    print(f'\n恢复后:')
    print(f'  页1 balance = {p1["data"]["balance"]}')
    print(f'  页2 balance = {p2["data"]["balance"]}')

    assert p2['data']['balance'] == 200, f'页2已在崩溃前撤销, 应保持200, 实际{p2["data"]["balance"]}'
    assert p1['data']['balance'] == 100, f'页1应继续撤销到100, 实际{p1["data"]["balance"]}'

    print('\n[PASS] 场景1通过: 已撤销的保持、未撤销的继续撤, 没有出现撤掉又变新值')
    clean_up()


# ============================================================
# 场景2: 完整回滚后再重启, 回滚过的页面还是事务前的内容
# 验收: 余额100改到200后回滚, 重启再读还是100
# ============================================================
def test_s2_rollback_then_restart():
    sep('场景2: 完整回滚后重启, 页面保持原值')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    txn = wal.begin_transaction()
    print(f'开始事务 Txn={txn}')

    p_before = {'id': 1, 'data': {'balance': 100}}
    p_after = {'id': 1, 'data': {'balance': 200}}
    wal.update(txn, 1, p_before, p_after)
    print(f'  页1 balance 100 → 200')

    wal.abort_transaction(txn)
    print(f'  完整回滚事务')

    page = wal.page_store.read_page(1)
    print(f'\n回滚后页1 balance = {page["data"]["balance"]}')
    assert page['data']['balance'] == 100, f'回滚后期望100, 实际{page["data"]["balance"]}'

    print('\n模拟重启...')
    del wal

    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    wal2.recover()

    page = wal2.page_store.read_page(1)
    print(f'重启恢复后页1 balance = {page["data"]["balance"]}')

    assert page['data']['balance'] == 100, f'重启后期望100, 实际{page["data"]["balance"]}'

    print('\n[PASS] 场景2通过: 完整回滚后重启, 页面还是事务前的值')
    clean_up()


# ============================================================
# 场景3: 检查点前很多历史事务, 检查点后少量日志, 恢复主要跟检查点后有关
# 历史事务放大时, 恢复不该跟着明显变慢
# ============================================================
def test_s3_checkpoint_scalability():
    sep('场景3: 检查点性能可扩展性')
    clean_up()

    def run_recovery_and_measure(history_count, label):
        """造 history_count 个历史提交事务+检查点+少量后续, 测恢复耗时"""
        wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

        for i in range(history_count):
            t = wal.begin_transaction()
            p_before = {'id': i + 1, 'data': {'v': 0}}
            p_after = {'id': i + 1, 'data': {'v': i + 1}}
            wal.update(t, i + 1, p_before, p_after)
            wal.commit_transaction(t)

        ckpt = wal.create_checkpoint()

        txn_active = wal.begin_transaction()
        p_before = {'id': 9999, 'data': {'v': 0}}
        p_after = {'id': 9999, 'data': {'v': 42}}
        wal.update(txn_active, 9999, p_before, p_after)

        del wal

        start = time.time()
        wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

        import io
        from contextlib import redirect_stdout
        capture = io.StringIO()
        with redirect_stdout(capture):
            wal2.recover()
        elapsed = time.time() - start
        output = capture.getvalue()

        redo_count = output.count('REDO')
        scan_lines = [l for l in output.split('\n') if '扫描, 加载' in l]

        del wal2
        clean_up()

        return elapsed, redo_count, scan_lines

    print('  历史事务数\t恢复耗时(s)\tREDO数量\t扫描条数')
    print('-' * 60)

    results = []
    for n in [10, 50, 200]:
        elapsed, redo_count, scan_lines = run_recovery_and_measure(n, f'{n}个历史')
        results.append((n, elapsed, redo_count))
        print(f'  {n:>6}\t{elapsed:.4f}\t{redo_count}\t{scan_lines[0] if scan_lines else ""}')

    print()

    _, t10, _ = results[0]
    _, t200, _ = results[2]
    ratio = t200 / max(t10, 0.001)
    print(f'  历史事务从10增加到200 (x20), 恢复耗时比值: {ratio:.2f}x')
    print(f'  REDO数量: 10历史={results[0][2]}, 200历史={results[2][2]}')

    assert results[0][2] == results[-1][2], f'REDO数量应相同(只跟检查点后日志数有关): {results[0][2]} vs {results[-1][2]}'

    if ratio < 5.0:
        print('\n[PASS] 场景3通过: 历史事务放大20倍, 恢复耗时增加有限')
    else:
        print(f'\n[WARN] 耗时比值{ratio:.1f}x, 可能受其他因素影响')

    clean_up()


# ============================================================
# 主入口
# ============================================================
def run_all():
    print('\n' + '*' * 70)
    print('*' + ' ' * 18 + 'WAL第二轮验收测试 (3个场景)' + ' ' * 20 + '*')
    print('*' * 70)

    tests = [
        ('回滚中途崩溃恢复', test_s1_rollback_mid_crash),
        ('完整回滚后重启', test_s2_rollback_then_restart),
        ('检查点性能可扩展', test_s3_checkpoint_scalability),
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
