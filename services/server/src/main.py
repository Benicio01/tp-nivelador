import os
import sys

import logger
import server
from lottery import Lottery

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
AGENCY_QUORUM_MIN = int(os.environ.get("AGENCY_QUORUM_MIN", "1"))


def main():
    logger.init()
    lottery = Lottery("/tmp/lottery.csv")
    s = server.Server(SERVER_HOST, SERVER_PORT, lottery, AGENCY_QUORUM_MIN)
    try:
        s.run()
    except Exception as e:
        logger.error("server-run", logger.LogResult.fail, "err", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
