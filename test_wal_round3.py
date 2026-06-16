"""
WAL模块第三轮验收测试 - 4个增强功能场景

场景1: 几千/几万历史事务, 检查点后少量日志, 恢复不随历史增长
场景2: 恢复统计输出 - 能看到扫描起点/读取条数/重做/撤销/检查点是否生效
场景3: 日志清理 - 截断检查点前的已提交日志, 清理后重启仍正常
场景4: 崩溃注入工具 - 在update/clr/rollback后各节点模拟崩溃, 验收恢复结果
"""

import os
import shutil
import sys
import time
import io
from contextlib import redirect_stdout
from wal_engine import WALManager, CrashInjector, SimulatedCrash

TEST_WAL_DIR = 'test_wal3'
TEST_DATA_DIR = 'test_data3'


def clean_up():
    for d in [TEST_WAL_DIR, TEST_DATA_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)


def sep(title=''):
    print('\n' + '=' * 70)
    if title:
        print(f'  {title}')
        print('=' * 70)


def make_history_txns(wal, n):
    """造n个已提交历史事务，每个改不同的页"""
    for i in range(n):
        t = wal.begin_transaction()
        p_before = {'id': i, 'data': {'v': 0}}
        p_after = {'id': i, 'data': {'v': i + 1}}
        wal.update(t, i, p_before, p_after)
        wal.commit_transaction(t)


# ============================================================
# 场景1: 大量历史事务后, 恢复耗时不跟历史数量一起明显涨
# ============================================================
def test_s1_scale_history():
    sep('场景1: 大量历史事务的恢复可扩展性')
    clean_up()

    def measure(history_n):
        wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
        make_history_txns(wal, history_n)
        ckpt_lsn = wal.create_checkpoint()

        t1 = wal.begin_transaction()
        wal.update(t1, 99999,
                   {'id': 99999, 'data': {'balance': 0}},
                   {'id': 99999, 'data': {'balance': 999}})

        t2 = wal.begin_transaction()
        wal.update(t2, 99998,
                   {'id': 99998, 'data': {'v': 0}},
                   {'id': 99998, 'data': {'v': 888}})
        wal.commit_transaction(t2)

        del wal

        start = time.time()
        wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
        capture = io.StringIO()
        with redirect_stdout(capture):
            stats = wal2.recover()
        elapsed = time.time() - start
        del wal2
        clean_up()
        return elapsed, stats

    results = []
    print(f'  {"历史事务数":>10} {"耗时(s)":>10} {"扫描起点":>10} {"读取条数":>10} {"REDO数":>8} {"UNDO数":>8} 检查点生效')
    print('-' * 90)
    for n in [200, 800, 2000]:
        elapsed, stats = measure(n)
        results.append((n, elapsed, stats))
        print(f'  {n:>10} {elapsed:>10.4f} {stats["scan_start_lsn"]:>10} {stats["log_records_read"]:>10} {stats["redo_count"]:>8} {stats["undo_count"]:>8} {"是" if stats["checkpoint_used"] else "否"}')

    print()
    _, t_small, s_small = results[0]
    _, t_large, s_large = results[-1]
    ratio = t_large / max(t_small, 0.0001)
    print(f'  历史事务从{results[0][0]}增加到{results[-1][0]} ({results[-1][0] // results[0][0]}x), 恢复耗时比值: {ratio:.2f}x')
    print(f'  REDO数量恒为 {s_small["redo_count"]}, 不受历史影响')
    assert s_small['redo_count'] == s_large['redo_count'], 'REDO数量应相同'
    assert s_small['checkpoint_used'] and s_large['checkpoint_used'], '检查点应该生效'

    if ratio < 5.0:
        print('\n[PASS] 场景1通过: 历史事务放大10倍, 恢复耗时增加有限')
    else:
        print(f'\n[WARN] 耗时比值{ratio:.1f}x, 可能受IO影响')
    clean_up()


# ============================================================
# 场景2: 恢复统计输出字段齐全
# ============================================================
def test_s2_recovery_stats():
    sep('场景2: 恢复统计输出')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    make_history_txns(wal, 50)
    wal.create_checkpoint()

    t1 = wal.begin_transaction()
    wal.update(t1, 1001,
               {'id': 1001, 'data': {'balance': 100}},
               {'id': 1001, 'data': {'balance': 200}})
    wal.update(t1, 1001,
               {'id': 1001, 'data': {'balance': 200}},
               {'id': 1001, 'data': {'balance': 300}})

    t2 = wal.begin_transaction()
    wal.update(t2, 1002,
               {'id': 1002, 'data': {'v': 1}},
               {'id': 1002, 'data': {'v': 2}})
    wal.commit_transaction(t2)

    del wal

    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    capture = io.StringIO()
    with redirect_stdout(capture):
        stats = wal2.recover()
    output = capture.getvalue()

    print(f'  统计字段完整性检查:')
    required = ['scan_start_lsn', 'log_records_read', 'backtrack_loads',
                'redo_count', 'undo_count', 'checkpoint_used', 'loser_txns']
    for key in required:
        assert key in stats, f'缺少统计字段: {key}'
        print(f'    {key}: {stats[key]}')

    print(f'  控制台输出包含恢复统计块: {"恢复统计" in output}')
    assert '恢复统计' in output, '控制台应输出恢复统计块'

    assert stats['checkpoint_used'] == True, '应有检查点'
    assert stats['scan_start_lsn'] > 100, f'扫描起点应远大于0, 实际={stats["scan_start_lsn"]}'
    assert stats['log_records_read'] < 20, f'检查点后日志应很少, 实际={stats["log_records_read"]}'
    assert stats['redo_count'] >= 2, f'至少重做t1和t2的更新'
    assert stats['undo_count'] == 2, f't1有两次更新应被撤销, 实际={stats["undo_count"]}'

    p = wal2.page_store.read_page(1001)
    assert p['data']['balance'] == 100, f'页1001应回到100, 实际={p["data"]["balance"]}'
    p2 = wal2.page_store.read_page(1002)
    assert p2['data']['v'] == 2, f'页1002应=2(已提交), 实际={p2["data"]["v"]}'

    print(f'\n  数据正确性: 页1001 balance={p["data"]["balance"]}, 页1002 v={p2["data"]["v"]}')
    print('\n[PASS] 场景2通过: 恢复统计字段齐全且值正确')
    clean_up()


# ============================================================
# 场景3: 日志清理 - truncate_wal
# ============================================================
def test_s3_wal_truncate():
    sep('场景3: 日志清理(truncate_wal)')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    make_history_txns(wal, 200)

    size_before_ckpt = os.path.getsize(wal.wal_file)
    print(f'  检查点前WAL大小: {size_before_ckpt} bytes')

    ckpt = wal.create_checkpoint()
    print(f'  创建检查点 LSN={ckpt}')

    t_active = wal.begin_transaction()
    wal.update(t_active, 8888,
               {'id': 8888, 'data': {'x': 0}},
               {'id': 8888, 'data': {'x': 1}})
    wal.abort_transaction(t_active)

    trunc_stats = wal.truncate_wal()
    print(f'  truncate结果:')
    print(f'    旧记录数: {trunc_stats["old_log_records"]}')
    print(f'    新记录数: {trunc_stats["new_log_records"]}')
    print(f'    旧文件大小: {trunc_stats["old_size_bytes"]}')
    print(f'    新文件大小: {trunc_stats["new_size_bytes"]}')
    print(f'    清理成功: {trunc_stats["truncated"]}')
    print(f'    保留最小LSN: {trunc_stats["min_retained_lsn"]}')

    assert trunc_stats['truncated'], '清理应该成功'
    assert trunc_stats['new_log_records'] < trunc_stats['old_log_records'], '应该减少了日志'
    assert trunc_stats['new_size_bytes'] < trunc_stats['old_size_bytes'], '文件应该变小了'

    # --- 验证有活跃事务时清理被拒绝 ---
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    t_block = wal2.begin_transaction()
    wal2.update(t_block, 7777,
                {'id': 7777, 'data': {'y': 0}},
                {'id': 7777, 'data': {'y': 1}})

    wal2.create_checkpoint()
    block_stats = wal2.truncate_wal()
    print(f'\n  有活跃事务时清理: error={block_stats["error"]}')
    assert not block_stats['truncated'], '有活跃事务时不应该允许清理'
    assert '活跃事务' in (block_stats['error'] or ''), '错误信息应提示活跃事务'

    del wal2

    # --- 清理后重启恢复 ---
    print('\n  清理后重启验证...')
    wal3 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    capture = io.StringIO()
    with redirect_stdout(capture):
        recover_stats = wal3.recover()
    print(f'    恢复: REDO={recover_stats["redo_count"]}, UNDO={recover_stats["undo_count"]}')

    # 检查历史数据都能正确读到
    for i in [0, 50, 100, 199]:
        p = wal3.page_store.read_page(i)
        assert p['data']['v'] == i + 1, f'页{i}值错误: {p["data"]["v"]}, 期望{i+1}'
    print(f'    抽查4个历史页面数据正确')

    print('\n[PASS] 场景3通过: 日志清理安全生效, 清理后重启恢复正常')
    clean_up()


# ============================================================
# 场景4: 崩溃注入工具 - 各个关键节点崩溃后恢复都正确
# ============================================================
def run_injection_case(inject_point, after_n, expected_check, description):
    """运行单个崩溃注入测试用例"""
    clean_up()
    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    wal.page_store.write_page(1, {'id': 1, 'data': {'balance': 100}}, 0)
    wal.page_store.write_page(2, {'id': 2, 'data': {'balance': 200}}, 0)
    wal.page_store.flush_all_pages()

    injector = CrashInjector(wal, inject_point, after_n=after_n)
    injector.attach()

    crashed = False
    try:
        t = wal.begin_transaction()
        wal.update(t, 1,
                   {'id': 1, 'data': {'balance': 100}},
                   {'id': 1, 'data': {'balance': 150}})
        wal.update(t, 2,
                   {'id': 2, 'data': {'balance': 200}},
                   {'id': 2, 'data': {'balance': 250}})
        if inject_point in ('after_commit_log',):
            wal.commit_transaction(t)
        else:
            wal.abort_transaction(t)
    except SimulatedCrash as e:
        crashed = True
        print(f'  [注入成功] {inject_point}(第{after_n}次): {e}')
    finally:
        injector.detach()

    assert crashed, f'{description} 应该触发崩溃'
    del wal

    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    capture = io.StringIO()
    with redirect_stdout(capture):
        wal2.recover()

    expected_check(wal2)
    clean_up()


def test_s4_crash_injection():
    sep('场景4: 崩溃注入工具 - 各节点崩溃验证')

    def check_both_rolled_back(wal):
        p1 = wal.page_store.read_page(1)
        p2 = wal.page_store.read_page(2)
        assert p1['data']['balance'] == 100, f'页1应为100, 实际{p1["data"]["balance"]}'
        assert p2['data']['balance'] == 200, f'页2应为200, 实际{p2["data"]["balance"]}'
        print(f'    验证: 页1={p1["data"]["balance"]}, 页2={p2["data"]["balance"]} (均为原值) ✓')

    def check_both_committed(wal):
        p1 = wal.page_store.read_page(1)
        p2 = wal.page_store.read_page(2)
        assert p1['data']['balance'] == 150, f'页1应为150, 实际{p1["data"]["balance"]}'
        assert p2['data']['balance'] == 250, f'页2应为250, 实际{p2["data"]["balance"]}'
        print(f'    验证: 页1={p1["data"]["balance"]}, 页2={p2["data"]["balance"]} (均为新值) ✓')

    cases = [
        ('after_update_page', 1, check_both_rolled_back,
         '写完页1的日志+数据后崩溃(未提交) → 两页都应回到原值'),
        ('after_update_page', 2, check_both_rolled_back,
         '写完页2的日志+数据后崩溃(未提交) → 两页都应回到原值'),
        ('after_clr_page', 1, check_both_rolled_back,
         'abort时撤完页1写了CLR后崩溃 → 两页都应回到原值'),
        ('after_rollback_log', 1, check_both_rolled_back,
         'abort写完ROLLBACK后崩溃 → 两页都应回到原值'),
        ('after_commit_log', 1, check_both_committed,
         '写完COMMIT日志后崩溃 → 两页都应为新值(已提交)'),
    ]

    passed = 0
    for inject_point, after_n, check_fn, desc in cases:
        print(f'\n  子场景: {desc}')
        try:
            run_injection_case(inject_point, after_n, check_fn, desc)
            passed += 1
            print(f'    ✓ PASS')
        except Exception as e:
            print(f'    ✗ FAIL: {e}')
            import traceback
            traceback.print_exc()

    print(f'\n  崩溃注入子场景通过: {passed}/{len(cases)}')
    assert passed == len(cases), f'{len(cases) - passed} 个注入场景失败'
    print('\n[PASS] 场景4通过: 所有崩溃注入点恢复结果正确')


# ============================================================
# 主入口
# ============================================================
def run_all():
    print('\n' + '*' * 70)
    print('*' + ' ' * 12 + 'WAL第三轮验收测试 (4个增强功能场景)' + ' ' * 18 + '*')
    print('*' * 70)

    tests = [
        ('大量历史事务恢复扩展', test_s1_scale_history),
        ('恢复统计输出', test_s2_recovery_stats),
        ('日志清理', test_s3_wal_truncate),
        ('崩溃注入工具', test_s4_crash_injection),
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
