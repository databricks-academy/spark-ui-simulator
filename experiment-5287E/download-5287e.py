from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #5287 - Persist/Exp #5287E-S - Persist, MEMORY_ONLY - Small Cluster",
        headless=True,
        version="v002-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()
