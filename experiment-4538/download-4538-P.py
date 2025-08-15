from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #4538-P - UDFs",
        headless=True,
        version="v002-P",
        languages=["scala","python"],
        has_lab=False)

capture.download_all()
