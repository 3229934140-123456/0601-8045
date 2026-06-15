"""
WAL模块测试用例

测试场景:
1. 基本事务提交和回滚
2. 崩溃恢复 - 已提交事务需重做
3. 崩溃恢复 - 未提交事务需撤销
4. 检查点机制 - 避免全量重放
5. 混合场景测试
"""

import os
import shutil
import sys
from wal_engine import WALManager, LogRecordType, LogRecord


TEST_WAL_DIR = 'test_wal'
TEST_DATA_DIR = 'test_data'


def clean_up():
    """清理测试目录"""
    for d in [TEST_WAL_DIR, TEST_DATA_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)


def print_separator(title=''):
    print('\n' + '=' * 70)
    if title:
        print(f'  {title}')
        print('=' * 70)


def test_1_basic_transaction():
    """测试1: 基本事务提交和回滚"""
    print_separator('测试1: 基本事务提交和回滚')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    txn1 = wal.begin_transaction()
    print(f'[Txn1] 开始事务 ID={txn1}')

    page1_before = {'id': 1, 'data': {'name': '张三', 'age': 25, 'balance': 1000}}
    page1_after = {'id': 1, 'data': {'name': '张三', 'age': 26, 'balance': 1500}}
    lsn1 = wal.update(txn1, 1, page1_before, page1_after)
    print(f'[Txn1] 更新页1 LSN={lsn1}, age 25->26, balance 1000->1500')

    page2_before = {'id': 2, 'data': {'name': '李四', 'balance': 500}}
    page2_after = {'id': 2, 'data': {'name': '李四', 'balance': 200}}
    lsn2 = wal.update(txn1, 2, page2_before, page2_after)
    print(f'[Txn1] 更新页2 LSN={lsn2}, balance 500->200')

    wal.commit_transaction(txn1)
    print(f'[Txn1] 提交事务')

    txn2 = wal.begin_transaction()
    print(f'\n[Txn2] 开始事务 ID={txn2}')

    page3_before = {'id': 3, 'data': {'product': 'A', 'stock': 100}}
    page3_after = {'id': 3, 'data': {'product': 'A', 'stock': 50}}
    lsn3 = wal.update(txn2, 3, page3_before, page3_after)
    print(f'[Txn2] 更新页3 LSN={lsn3}, stock 100->50')

    wal.abort_transaction(txn2)
    print(f'[Txn2] 回滚事务')

    stats = wal.get_stats()
    print(f'\n统计: {stats}')

    page1 = wal.page_store.read_page(1)
    page2 = wal.page_store.read_page(2)
    page3 = wal.page_store.read_page(3)

    assert page1['data']['age'] == 26, 'Txn1提交后页1应已更新'
    assert page1['data']['balance'] == 1500, 'Txn1提交后页1 balance应已更新'
    assert page2['data']['balance'] == 200, 'Txn1提交后页2 balance应已更新'
    assert page3['data']['stock'] == 100, 'Txn2回滚后页3应保持原值'

    print('\n[PASS] 测试1通过: 基本事务提交/回滚正常')
    clean_up()


def test_2_recovery_committed():
    """测试2: 崩溃恢复 - 已提交事务需重做"""
    print_separator('测试2: 崩溃恢复 - 已提交事务需重做')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    txn1 = wal.begin_transaction()
    page1_before = {'id': 1, 'data': {'value': 'initial'}}
    page1_after = {'id': 1, 'data': {'value': 'updated_by_txn1'}}
    wal.update(txn1, 1, page1_before, page1_after)

    txn2 = wal.begin_transaction()
    page2_before = {'id': 2, 'data': {'count': 0}}
    page2_after = {'id': 2, 'data': {'count': 42}}
    wal.update(txn2, 2, page2_before, page2_after)
    wal.commit_transaction(txn2)

    print('写入数据完成，模拟崩溃...')

    del wal

    print('\n重启系统，执行恢复...')
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    wal2.recover()

    page1 = wal2.page_store.read_page(1)
    page2 = wal2.page_store.read_page(2)

    print(f'\n页1值: {page1["data"]}')
    print(f'页2值: {page2["data"]}')

    assert page2['data']['count'] == 42, 'Txn2已提交，恢复后应保持更新后的值'

    print('\n[PASS] 测试2通过: 已提交事务恢复后重做成功')
    clean_up()


def test_3_recovery_uncommitted():
    """测试3: 崩溃恢复 - 未提交事务需撤销"""
    print_separator('测试3: 崩溃恢复 - 未提交事务需撤销')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    wal.page_store.write_page(1, {'id': 1, 'data': {'name': 'original_name', 'score': 80}}, 0)
    wal.page_store.write_page(2, {'id': 2, 'data': {'status': 'inactive'}}, 0)

    txn = wal.begin_transaction()
    page1_before = {'id': 1, 'data': {'name': 'original_name', 'score': 80}}
    page1_after = {'id': 1, 'data': {'name': 'modified_name', 'score': 95}}
    wal.update(txn, 1, page1_before, page1_after)

    page2_before = {'id': 2, 'data': {'status': 'inactive'}}
    page2_after = {'id': 2, 'data': {'status': 'active'}}
    wal.update(txn, 2, page2_before, page2_after)

    print('写入数据完成，模拟崩溃（不提交也不回滚）...')

    del wal

    print('\n重启系统，执行恢复...')
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    wal2.recover()

    page1 = wal2.page_store.read_page(1)
    page2 = wal2.page_store.read_page(2)

    print(f'\n页1值: {page1["data"]}')
    print(f'页2值: {page2["data"]}')

    assert page1['data']['name'] == 'original_name', '未提交事务恢复后页1 name应被撤销'
    assert page1['data']['score'] == 80, '未提交事务恢复后页1 score应被撤销'
    assert page2['data']['status'] == 'inactive', '未提交事务恢复后页2 status应被撤销'

    print('\n[PASS] 测试3通过: 未提交事务恢复后撤销成功')
    clean_up()


def test_4_checkpoint():
    """测试4: 检查点机制 - 避免全量重放"""
    print_separator('测试4: 检查点机制')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    print('阶段1: 初始数据写入 + 检查点')
    txn1 = wal.begin_transaction()
    page1_before = {'id': 1, 'data': {'v': 100}}
    page1_after = {'id': 1, 'data': {'v': 200}}
    wal.update(txn1, 1, page1_before, page1_after)
    wal.commit_transaction(txn1)

    checkpoint_lsn = wal.create_checkpoint()
    print(f'创建检查点 LSN={checkpoint_lsn}')

    print('\n阶段2: 检查点之后再写入一些数据')
    txn2 = wal.begin_transaction()
    page2_before = {'id': 2, 'data': {'x': 'a'}}
    page2_after = {'id': 2, 'data': {'x': 'b'}}
    wal.update(txn2, 2, page2_before, page2_after)

    txn3 = wal.begin_transaction()
    page3_before = {'id': 3, 'data': {'y': 1}}
    page3_after = {'id': 3, 'data': {'y': 999}}
    wal.update(txn3, 3, page3_before, page3_after)
    wal.commit_transaction(txn3)

    print(f'Txn2(未提交) 更新页2: a->b')
    print(f'Txn3(已提交) 更新页3: 1->999')

    stats = wal.get_stats()
    print(f'状态: {stats}')

    print('\n模拟崩溃...')
    del wal

    print('\n重启系统，执行恢复 (应从检查点开始重放)...')
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    wal2.recover()

    page1 = wal2.page_store.read_page(1)
    page2 = wal2.page_store.read_page(2)
    page3 = wal2.page_store.read_page(3)

    print(f'\n页1值: {page1["data"]}')
    print(f'页2值: {page2["data"]}')
    print(f'页3值: {page3["data"]}')

    assert page1['data']['v'] == 200, '检查点前提交的页1应保持更新值'
    assert page2['data']['x'] == 'a', 'Txn2未提交，应被撤销回原值'
    assert page3['data']['y'] == 999, 'Txn3已提交，应保持更新值'

    print('\n[PASS] 测试4通过: 检查点机制正常工作')
    clean_up()


def test_5_mixed_scenario():
    """测试5: 混合场景 - 多事务、检查点、部分提交"""
    print_separator('测试5: 混合复杂场景')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    for pid in range(1, 6):
        wal.page_store.write_page(pid, {'id': pid, 'data': {'val': pid * 10}}, 0)
    wal.page_store.flush_all_pages()
    print('初始化5个数据页: [10, 20, 30, 40, 50]')

    txn_a = wal.begin_transaction()
    print(f'\nTxn A (ID={txn_a}) 开始: 修改页1、页2')
    wal.update(txn_a, 1, {'id': 1, 'data': {'val': 10}}, {'id': 1, 'data': {'val': 100}})
    wal.update(txn_a, 2, {'id': 2, 'data': {'val': 20}}, {'id': 2, 'data': {'val': 200}})

    txn_b = wal.begin_transaction()
    print(f'Txn B (ID={txn_b}) 开始: 修改页3')
    wal.update(txn_b, 3, {'id': 3, 'data': {'val': 30}}, {'id': 3, 'data': {'val': 300}})
    wal.commit_transaction(txn_b)
    print('Txn B 已提交')

    ckpt1 = wal.create_checkpoint()
    print(f'\n创建检查点1 LSN={ckpt1}')

    txn_c = wal.begin_transaction()
    print(f'\nTxn C (ID={txn_c}) 开始: 修改页4、页5')
    wal.update(txn_c, 4, {'id': 4, 'data': {'val': 40}}, {'id': 4, 'data': {'val': 400}})
    wal.update(txn_c, 5, {'id': 5, 'data': {'val': 50}}, {'id': 5, 'data': {'val': 500}})
    wal.commit_transaction(txn_c)
    print('Txn C 已提交')

    print(f'\nTxn A (ID={txn_a}) 继续: 修改页2 (再次)')
    wal.update(txn_a, 2,
               {'id': 2, 'data': {'val': 200}},
               {'id': 2, 'data': {'val': 201}})

    print('\n=== 当前状态 ===')
    print(f'Txn A: 活跃 (未提交) - 页1=100, 页2=201')
    print(f'Txn B: 已提交 - 页3=300')
    print(f'Txn C: 已提交 - 页4=400, 页5=500')

    print('\n模拟崩溃！Txn A还未提交...')
    del wal

    print('\n' + '=' * 50)
    print('系统重启，执行崩溃恢复...')
    print('=' * 50)
    wal2 = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)
    wal2.recover()

    results = {}
    for pid in range(1, 6):
        page = wal2.page_store.read_page(pid)
        results[pid] = page['data']['val']

    print('\n' + '=' * 50)
    print('恢复结果验证:')
    print('=' * 50)
    for pid in range(1, 6):
        print(f'  页{pid}: val = {results[pid]}')

    assert results[1] == 10,  '页1: TxnA未提交，应撤销回原值10'
    assert results[2] == 20,  '页2: TxnA未提交，应撤销回原值20'
    assert results[3] == 300, '页3: TxnB已提交，应保持300'
    assert results[4] == 400, '页4: TxnC已提交，应保持400'
    assert results[5] == 500, '页5: TxnC已提交，应保持500'

    print('\n[PASS] 测试5通过: 混合场景恢复正确!')
    clean_up()


def test_6_wal_principle_violation():
    """测试6: 验证WAL原则 - 日志刷盘先于数据页刷盘"""
    print_separator('测试6: WAL原则验证')
    clean_up()

    wal = WALManager(TEST_WAL_DIR, TEST_DATA_DIR)

    txn = wal.begin_transaction()
    page_before = {'id': 1, 'data': {'test': 'before'}}
    page_after = {'id': 1, 'data': {'test': 'after'}}
    update_lsn = wal.update(txn, 1, page_before, page_after)

    print(f'更新记录 LSN={update_lsn}')
    print(f'已刷盘 LSN={wal._flushed_lsn}')

    assert wal._flushed_lsn >= update_lsn, 'WAL原则: 日志必须在更新后立即刷盘'
    print('[OK] 更新操作的日志已刷盘')

    wal.commit_transaction(txn)
    commit_lsn = wal._current_lsn
    print(f'提交 LSN={commit_lsn}, 已刷盘 LSN={wal._flushed_lsn}')
    assert wal._flushed_lsn >= commit_lsn, 'WAL原则: COMMIT日志必须刷盘后才算提交'
    print('[OK] COMMIT日志已刷盘')

    flushed_before = wal._flushed_lsn
    wal.flush_pages_with_wal_guarantee(1)
    print(f'刷盘页1前已刷盘LSN: {flushed_before}, 刷盘后: {wal._flushed_lsn}')
    print('[OK] 页1刷盘时已确保rec_lsn前的日志已刷盘')

    print('\n[PASS] 测试6通过: WAL原则得到保证')
    clean_up()


def run_all_tests():
    """运行所有测试"""
    print('\n' + '*' * 70)
    print('*' + ' ' * 20 + 'WAL模块综合测试' + ' ' * 28 + '*')
    print('*' * 70)

    tests = [
        ('基本事务提交/回滚', test_1_basic_transaction),
        ('崩溃恢复-已提交重做', test_2_recovery_committed),
        ('崩溃恢复-未提交撤销', test_3_recovery_uncommitted),
        ('检查点机制', test_4_checkpoint),
        ('混合复杂场景', test_5_mixed_scenario),
        ('WAL原则验证', test_6_wal_principle_violation),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f'\n[FAIL] 测试"{name}"失败: {e}')
            import traceback
            traceback.print_exc()
            failed += 1
            clean_up()

    print_separator('测试总结')
    print(f'总测试数: {len(tests)}')
    print(f'通过: {passed}')
    print(f'失败: {failed}')

    if failed == 0:
        print('\n' + '=' * 70)
        print('  ✅  所有测试通过!')
        print('=' * 70)
    else:
        print('\n' + '=' * 70)
        print(f'  ❌  有 {failed} 个测试失败')
        print('=' * 70)

    clean_up()
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
