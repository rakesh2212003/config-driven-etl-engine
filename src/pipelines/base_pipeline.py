from src.core.logger import get_logger
import time


class BasePipeline:

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    def extract(self):
        raise NotImplementedError

    def transform(self, df):
        raise NotImplementedError

    def load(self, df):
        raise NotImplementedError

    def run(self):
        start_time = time.time()

        try:
            self.logger.info("🚀 Pipeline started")

            # 🔹 Extract
            df = self.extract()
            row_count = df.count()
            self.logger.info(f"Extracted rows: {row_count}")

            # 🔹 Transform
            df = self.transform(df)

            # 🔹 Load
            self.load(df)

            duration = round(time.time() - start_time, 2)
            self.logger.info(f"✅ Pipeline completed in {duration}s")

            return True

        except Exception as e:
            duration = round(time.time() - start_time, 2)

            self.logger.error(
                f"❌ Pipeline failed after {duration}s: {str(e)}",
                exc_info=True
            )

            return False