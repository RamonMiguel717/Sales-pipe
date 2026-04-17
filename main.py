import argparse
import logging
import sys
from pathlib import Path

from core.logging_config import setup_logging
from pipe.sales import principal


def parse_args() -> argparse.Namespace:
    """Parse the CLI arguments for the pipeline entrypoint."""
    parser = argparse.ArgumentParser(
        description="Sales Pipe - Analytics Engineering pipeline",
    )
    parser.add_argument(
        "--persist-mysql",
        action="store_true",
        help="Persist Bronze, Silver and Gold in MySQL.",
    )
    parser.add_argument(
        "--entry-path",
        type=str,
        default=None,
        help="Directory containing the source CSV files.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for MySQL inserts.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Application log level.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the pipeline from the command line."""
    args = parse_args()
    setup_logging(level=getattr(logging, args.log_level))
    logger = logging.getLogger(__name__)

    kwargs: dict[str, object] = {}
    if args.entry_path:
        kwargs["entry_path"] = Path(args.entry_path)

    try:
        tables = principal(
            persist_mysql=args.persist_mysql,
            batch_size=args.batch_size,
            **kwargs,
        )
    except FileNotFoundError as exc:
        logger.error("Source files not found: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Unexpected pipeline error: %s", exc, exc_info=True)
        sys.exit(1)

    logger.info("Pipeline completed successfully")
    for name, dataframe in tables.items():
        logger.info("%-35s %d rows x %d columns", name, len(dataframe), len(dataframe.columns))


if __name__ == "__main__":
    main()
