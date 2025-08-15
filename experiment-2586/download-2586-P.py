from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #2586-P - Manual Compaction",
        headless=True,
        version="v002-P",
        languages=["scala","python"],
        has_lab=False)

capture.download_all()