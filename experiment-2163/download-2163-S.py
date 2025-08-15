from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #2163-S - IO Cache",
        headless=True,
        version="v002-S",
        languages=["scala"],
        has_lab=False)

capture.all()