from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #3799 - Join-Filter/Exp #3799A-S - Filtered Join, Standard",
        headless=True,
        version="v002-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()

