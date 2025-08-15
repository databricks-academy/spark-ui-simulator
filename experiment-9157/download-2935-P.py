from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Repos/student@azuredatabrickstraining.onmicrosoft.com/spark-ui-simulator-experiments/Exp #9157-S - Shuffle Hash Join",
        headless=True,
        version="v003-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()

