import os
import sys

import logger
import server
from lottery import Lottery

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
AGENCY_QUORUM_MIN = int(os.environ.get("AGENCY_QUORUM_MIN", "1"))
STORAGE_PATH = os.environ.get("STORAGE_PATH", "/tmp/lottery.csv")

def main():
    logger.init()
    lottery = Lottery(STORAGE_PATH)
    s = server.Server(SERVER_HOST, SERVER_PORT, lottery, AGENCY_QUORUM_MIN)
    try:
        s.run()
    except Exception as e:
        logger.error("server-run", logger.LogResult.fail, "err", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
