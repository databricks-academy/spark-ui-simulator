from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #4538-S - UDFs",
        headless=True,
        version="v002-S",
        languages=["scala","python"],
        has_lab=False)

capture.download_all()
