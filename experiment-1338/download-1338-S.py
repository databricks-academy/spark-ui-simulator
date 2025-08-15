from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Repos/Shared/spark-ui-simulator-experiments/Exp #1338-S - Bloom Filter Query - 1000",
        headless=True,
        version="v004-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()