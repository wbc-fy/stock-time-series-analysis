"""
数据采集服务 — CLI 入口

支持的命令：
    daily        采集指定交易日数据
    history      回补指定日期范围数据
    basic        更新股票基础信息
    retry-failed 重试失败任务
    replay-daily-events 从本地 MySQL 向 Kafka 重放已有日线

示例：
    python main.py daily --date 2026-07-03
    python main.py daily --date 2026-07-03 --source akshare
    python main.py history --start 2026-01-01 --end 2026-07-03
    python main.py basic
    python main.py retry-failed
    python main.py replay-daily-events --start 2026-01-01 --end 2026-07-03
"""

import argparse
import os
import sys

# ── 路径设置 ─────────────────────────────────────────────────
# 确保 app 包可被导入（将服务根目录加入 sys.path）
_APP_DIR = os.path.dirname(os.path.abspath(__file__))               # .../app/
_SERVICE_ROOT = os.path.dirname(_APP_DIR)                            # .../python-collector/
_PYTHON_SERVICES_ROOT = os.path.dirname(_SERVICE_ROOT)               # .../python-services/
_STOCK_ANALYSIS_ROOT = os.path.join(_PYTHON_SERVICES_ROOT, 'stock-analysis-app')

for _p in (_SERVICE_ROOT, _STOCK_ANALYSIS_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from app.config import (
    DATABASE_CONFIG,
    COLLECTOR_SOURCE,
    COLLECTOR_OUTPUT_MODE,
    COLLECTION_CONFIG,
    safe_config_repr,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ── 数据库初始化 ─────────────────────────────────────────────

def _init_database():
    """
    初始化数据库连接并确保表存在。

    使用项目共享的 DatabaseConnector，同时确保 collection_task 表已创建。
    """
    from database.db_connector import DatabaseConnector
    from database.models import CollectionTask  # noqa: F401 — 确保 ORM 注册

    db = DatabaseConnector(DATABASE_CONFIG)
    db.create_tables()
    logger.info("数据库初始化完成")
    logger.info(f"配置: {safe_config_repr()}")
    return db


def _create_collector(source: str):
    """
    根据配置创建数据采集器。

    Args:
        source: 'tushare' 或 'akshare'

    Returns:
        BaseCollector 实例
    """
    if source == 'tushare':
        from app.collectors.tushare_collector import TushareCollector
        return TushareCollector()
    elif source == 'akshare':
        from app.collectors.akshare_collector import AkShareCollector
        return AkShareCollector()
    else:
        raise ValueError(f"不支持的数据源: {source}，可选: tushare, akshare")


# ── 子命令实现 ───────────────────────────────────────────────

def cmd_daily(args):
    """daily 子命令：采集指定交易日数据"""
    db = _init_database()
    source = args.source or COLLECTOR_SOURCE

    logger.info(f"执行单日采集: {args.date}, 数据源: {source}")

    collector = _create_collector(source)

    with db.session_scope() as session:
        from app.repositories.stock_repository import StockRepository
        from app.repositories.task_repository import TaskRepository
        from app.jobs.daily_collection_job import DailyCollectionJob
        from app.outputs.output_factory import OutputFactory
        from app.utils.date_utils import load_trade_calendar

        stock_repo = StockRepository(session)
        task_repo = TaskRepository(session)
        trade_calendar = load_trade_calendar(collector)
        output = OutputFactory.create(
            mode=COLLECTOR_OUTPUT_MODE,
            stock_repository=stock_repo,
        )

        logger.info(f"Daily output mode: {COLLECTOR_OUTPUT_MODE}")
        job = DailyCollectionJob(
            collector,
            stock_repo,
            task_repo,
            trade_calendar,
            output=output,
        )
        result = job.execute(args.date)

    if result['success']:
        if result['skipped']:
            logger.info(f"✓ {args.date} 已跳过（非交易日或已成功）")
        else:
            logger.info(
                f"✓ {args.date} 采集完成: {result['record_count']} 条, "
                f"耗时 {result['elapsed']:.1f}s"
            )
        return 0
    else:
        logger.error(f"✗ {args.date} 采集失败: {result.get('error', '未知')}")
        return 1


def cmd_history(args):
    """history 子命令：回补指定日期范围数据"""
    db = _init_database()
    source = args.source or COLLECTOR_SOURCE

    logger.info(f"执行历史回补: {args.start} ~ {args.end}, 数据源: {source}")

    collector = _create_collector(source)

    with db.session_scope() as session:
        from app.repositories.stock_repository import StockRepository
        from app.repositories.task_repository import TaskRepository
        from app.jobs.history_backfill_job import HistoryBackfillJob
        from app.outputs.output_factory import OutputFactory

        stock_repo = StockRepository(session)
        task_repo = TaskRepository(session)
        output = OutputFactory.create(
            mode=COLLECTOR_OUTPUT_MODE,
            stock_repository=stock_repo,
        )

        job = HistoryBackfillJob(
            collector,
            stock_repo,
            task_repo,
            output=output,
        )
        result = job.execute(args.start, args.end)

    if result['success']:
        logger.info("✓ 历史回补全部完成")
        return 0
    else:
        logger.warning(
            f"⚠ 历史回补部分完成: "
            f"{result['success_count']} 成功, "
            f"{result['failed_count']} 失败"
        )
        return 1


def cmd_replay_daily_events(args):
    """只读本地 stock_daily，向现有 Kafka 日线主题重放事件。"""
    from database.db_connector import DatabaseConnector
    from app.jobs.daily_event_replay_job import (
        DailyEventReplayJob,
        ReplayFailure,
        normalize_replay_ts_code,
    )
    from app.kafka.producer import StockKafkaProducer
    from app.repositories.stock_repository import StockRepository
    from app.utils.date_utils import parse_date

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if start_date > end_date:
        raise ValueError('start_date 不能晚于 end_date')
    if args.batch_size <= 0:
        raise ValueError('batch-size 必须大于 0')
    ts_code = normalize_replay_ts_code(args.ts_code)
    replay_scope = ts_code or 'ALL'

    db = None
    producer = None
    result = None
    failure = None
    try:
        # Do not use _init_database(): it calls create_tables(). Replay is read-only.
        db = DatabaseConnector(DATABASE_CONFIG)
        producer = StockKafkaProducer()
        with db.session_scope() as session:
            result = DailyEventReplayJob(
                StockRepository(session), producer
            ).execute(start_date, end_date, args.batch_size, ts_code)
    except ReplayFailure as exc:
        failure = exc
    except Exception:
        # Exceptions from the connector/producer may contain credentials or payloads.
        failure = ReplayFailure(0, 0)
    finally:
        if producer is not None:
            try:
                producer.close(flush=False)
            except Exception:
                if failure is None:
                    failure = ReplayFailure(
                        result['published_count'] if result else 0,
                        result['batch_count'] if result else 0,
                    )
        if db is not None:
            try:
                db.close()
            except Exception:
                if failure is None:
                    failure = ReplayFailure(
                        result['published_count'] if result else 0,
                        result['batch_count'] if result else 0,
                    )

    if failure is not None:
        logger.error(
            'Kafka 历史重放失败 [%s..%s, tsCode=%s]: 已确认 %s 条、%s 批',
            start_date, end_date, replay_scope,
            failure.published_count, failure.batch_count,
        )
        return 1
    logger.info(
        'Kafka 历史重放完成 [%s..%s, tsCode=%s]: %s 条、%s 批',
        start_date, end_date, replay_scope,
        result['published_count'], result['batch_count'],
    )
    return 0


def _stock_codes_for_daily_basic(source, stock_repo):
    """AkShare 逐股采集时从本地 stock_basic 提供代码清单。"""
    if source != 'akshare':
        return None
    frame = stock_repo.load_stock_basic_df()
    if frame.empty or 'ts_code' not in frame.columns:
        return []
    return frame['ts_code'].dropna().drop_duplicates().tolist()


def cmd_daily_basic(args):
    """daily-basic 子命令：采集指定交易日的估值和换手率。"""
    db = _init_database()
    source = args.source or COLLECTOR_SOURCE
    collector = _create_collector(source)

    with db.session_scope() as session:
        from app.jobs.daily_basic_collection_job import DailyBasicCollectionJob
        from app.repositories.market_data_repository import MarketDataRepository
        from app.repositories.stock_repository import StockRepository
        from app.repositories.task_repository import TaskRepository

        market_repo = MarketDataRepository(session)
        task_repo = TaskRepository(session)
        stock_codes = _stock_codes_for_daily_basic(source, StockRepository(session))
        result = DailyBasicCollectionJob(
            collector, market_repo, task_repo
        ).execute(args.date, stock_codes=stock_codes)

    if result['success']:
        logger.info('✓ %s 每日指标完成: %s 条', args.date, result['record_count'])
        return 0
    logger.error('✗ %s 每日指标失败: %s', args.date, result.get('error'))
    return 1


def cmd_daily_basic_history(args):
    """daily-basic-history 子命令：按行情表已有日期历史补采。"""
    db = _init_database()
    source = args.source or COLLECTOR_SOURCE
    collector = _create_collector(source)

    with db.session_scope() as session:
        from app.jobs.daily_basic_backfill_job import DailyBasicBackfillJob
        from app.repositories.market_data_repository import MarketDataRepository
        from app.repositories.stock_repository import StockRepository
        from app.repositories.task_repository import TaskRepository

        market_repo = MarketDataRepository(session)
        task_repo = TaskRepository(session)
        stock_codes = _stock_codes_for_daily_basic(source, StockRepository(session))
        result = DailyBasicBackfillJob(
            collector,
            market_repo,
            task_repo,
            stock_codes=stock_codes,
            checkpoint=session.commit,
        ).execute(args.start, args.end)

    logger.info(
        '每日指标补采完成: success=%s failed=%s skipped=%s',
        result['success_count'], result['failed_count'], result['skipped_count'],
    )
    return 0 if result['failed_count'] == 0 else 1


def cmd_constituent(args):
    """constituent 子命令：保存指数或行业成分股快照。"""
    db = _init_database()
    source = args.source or COLLECTOR_SOURCE
    collector = _create_collector(source)

    with db.session_scope() as session:
        from app.jobs.constituent_collection_job import ConstituentCollectionJob
        from app.repositories.market_data_repository import MarketDataRepository
        from app.repositories.task_repository import TaskRepository

        result = ConstituentCollectionJob(
            collector,
            MarketDataRepository(session),
            TaskRepository(session),
        ).execute(args.group_type, args.code, args.date)

    if result['success']:
        logger.info('✓ 成分股快照完成: %s 条', result['record_count'])
        return 0
    logger.error('✗ 成分股快照失败: %s', result.get('error'))
    return 1


def cmd_daily_market(args):
    """分别提交 OHLCV 和 daily-basic，任一失败不回滚另一项。"""
    daily_exit = cmd_daily(args)
    daily_basic_exit = cmd_daily_basic(args)
    logger.info(
        'daily-market 结果: daily=%s daily_basic=%s',
        'SUCCESS' if daily_exit == 0 else 'FAILED',
        'SUCCESS' if daily_basic_exit == 0 else 'FAILED',
    )
    return 0 if daily_exit == 0 and daily_basic_exit == 0 else 1


def cmd_basic(args):
    """basic 子命令：更新股票基础信息"""
    db = _init_database()
    source = args.source or COLLECTOR_SOURCE

    logger.info(f"更新股票基础信息, 数据源: {source}")

    collector = _create_collector(source)

    from app.models.collection_task import CollectionTaskRecord, TaskType, TaskStatus

    with db.session_scope() as session:
        from app.repositories.stock_repository import StockRepository
        from app.repositories.task_repository import TaskRepository

        stock_repo = StockRepository(session)
        task_repo = TaskRepository(session)

        # 创建任务记录
        today = __import__('datetime').datetime.now().strftime('%Y%m%d')
        task = CollectionTaskRecord(
            task_type=TaskType.BASIC.value,
            business_date=today,
            source=source,
        )
        task_id = task_repo.create_task(task)
        task_repo.update_status(task_id, TaskStatus.RUNNING.value)

        try:
            df = collector.collect_basic()
            if df.empty:
                task_repo.update_status(
                    task_id, TaskStatus.FAILED.value,
                    error_message="获取股票基础信息为空",
                )
                logger.error("获取股票基础信息为空")
                return 1

            # 标准化写入
            records = df.to_dict('records')
            count = stock_repo.save_stock_basic(records)

            task_repo.update_status(
                task_id, TaskStatus.SUCCESS.value,
                record_count=count,
            )
            logger.info(f"✓ 股票基础信息更新完成: {count} 只")
            return 0

        except Exception as e:
            task_repo.update_status(
                task_id, TaskStatus.FAILED.value,
                error_message=str(e)[:500],
            )
            logger.error(f"✗ 股票基础信息更新失败: {e}")
            return 1


def cmd_retry_failed(args):
    """retry-failed 子命令：重试失败任务"""
    db = _init_database()
    source = args.source or COLLECTOR_SOURCE

    logger.info(f"执行失败任务重试, 数据源: {source}")

    collector = _create_collector(source)

    with db.session_scope() as session:
        from app.repositories.stock_repository import StockRepository
        from app.repositories.task_repository import TaskRepository
        from app.jobs.retry_failed_job import RetryFailedJob
        from app.outputs.output_factory import OutputFactory

        stock_repo = StockRepository(session)
        task_repo = TaskRepository(session)
        output = OutputFactory.create(
            mode=COLLECTOR_OUTPUT_MODE,
            stock_repository=stock_repo,
        )

        job = RetryFailedJob(
            collector,
            stock_repo,
            task_repo,
            output=output,
        )
        result = job.execute()

    if result['success']:
        logger.info("✓ 失败任务重试完成")
        return 0
    else:
        logger.warning(
            f"⚠ 重试部分完成: "
            f"{result['recovered']} 恢复, "
            f"{result['still_failed']} 仍失败"
        )
        return 1


# ── 主函数 ───────────────────────────────────────────────────

def build_parser():
    """构建命令行解析器，便于测试所有命令契约。"""
    parser = argparse.ArgumentParser(
        description='数据采集服务 V0.2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
子命令说明:
  daily         采集指定交易日数据
  history       回补指定日期范围数据
  basic         更新股票基础信息
  retry-failed  重试失败任务
  daily-basic  采集每日估值和换手率
  daily-basic-history  历史补采每日指标
  constituent  采集指数或行业成分股快照
  daily-market 同日依次采集 OHLCV 和每日指标
  replay-daily-events 按交易日顺序向 Kafka 重放本地日线

示例:
  python main.py daily --date 2026-07-03
  python main.py daily --date 2026-07-03 --source akshare
  python main.py history --start 2026-01-01 --end 2026-07-03
  python main.py basic
  python main.py retry-failed
  python main.py retry-failed --source tushare
  python main.py replay-daily-events --start 2026-01-01 --end 2026-07-03
        """,
    )

    # 全局参数
    parser.add_argument(
        '--source', type=str, default=None,
        choices=['tushare', 'akshare'],
        help='数据源（默认使用 COLLECTOR_SOURCE 环境变量）',
    )

    subparsers = parser.add_subparsers(dest='command', help='运行模式')

    # daily
    p_daily = subparsers.add_parser('daily', help='采集指定交易日数据')
    p_daily.add_argument(
        '--date', type=str, required=True,
        help='交易日期（YYYYMMDD 或 YYYY-MM-DD）',
    )

    # history
    p_history = subparsers.add_parser('history', help='回补历史数据')
    p_history.add_argument(
        '--start', type=str, required=True,
        help='开始日期（YYYYMMDD 或 YYYY-MM-DD）',
    )
    p_history.add_argument(
        '--end', type=str, required=True,
        help='结束日期（YYYYMMDD 或 YYYY-MM-DD）',
    )

    p_replay = subparsers.add_parser(
        'replay-daily-events', help='按交易日顺序向 Kafka 重放已有日线行情'
    )
    p_replay.add_argument('--start', required=True)
    p_replay.add_argument('--end', required=True)
    p_replay.add_argument('--batch-size', type=int, default=5000)
    p_replay.add_argument(
        '--ts-code', default=None,
        help='仅重放一只股票（如 000001.SZ）；省略则重放全市场',
    )

    # basic
    subparsers.add_parser('basic', help='更新股票基础信息')

    # retry-failed
    subparsers.add_parser('retry-failed', help='重试失败任务')

    p_daily_basic = subparsers.add_parser(
        'daily-basic', help='采集指定交易日的估值和换手率'
    )
    p_daily_basic.add_argument('--date', type=str, required=True)

    p_daily_basic_history = subparsers.add_parser(
        'daily-basic-history', help='按已有行情交易日补采每日指标'
    )
    p_daily_basic_history.add_argument('--start', type=str, required=True)
    p_daily_basic_history.add_argument('--end', type=str, required=True)

    p_constituent = subparsers.add_parser(
        'constituent', help='采集指数或行业成分股快照'
    )
    p_constituent.add_argument(
        '--type', dest='group_type', choices=['index', 'industry'], required=True
    )
    p_constituent.add_argument('--code', type=str, required=True)
    p_constituent.add_argument('--date', type=str, required=True)

    p_daily_market = subparsers.add_parser(
        'daily-market', help='分别采集 OHLCV 和每日指标'
    )
    p_daily_market.add_argument('--date', type=str, required=True)

    return parser


def main():
    """采集服务 CLI 入口"""
    parser = build_parser()

    # 解析
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # 分发
    dispatch = {
        'daily': cmd_daily,
        'history': cmd_history,
        'replay-daily-events': cmd_replay_daily_events,
        'basic': cmd_basic,
        'retry-failed': cmd_retry_failed,
        'daily-basic': cmd_daily_basic,
        'daily-basic-history': cmd_daily_basic_history,
        'constituent': cmd_constituent,
        'daily-market': cmd_daily_market,
    }

    handler = dispatch.get(args.command)
    if handler:
        exit_code = handler(args)
        sys.exit(exit_code or 0)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
