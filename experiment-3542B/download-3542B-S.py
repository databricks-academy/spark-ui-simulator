from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Curriculum/Spark-UI-Simulator/Exp #3542 - Join/Exp #3542B-S - Simple Join, Partioned",
        headless=True,
        version="v002-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()
