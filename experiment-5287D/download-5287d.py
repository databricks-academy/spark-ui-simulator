from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #5287 - Persist/Exp #5287D-S - Persist, MEMORY_AND_DISK - Small Cluster",
        headless=True,
        version="v002-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()
