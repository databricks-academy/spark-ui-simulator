from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Repos/student@azuredatabrickstraining.onmicrosoft.com/spark-ui-simulator-experiments/Exp #2935-P - Predicate Pushdown",
        headless=True,
        version="v003-P",
        languages=["python"],
        has_lab=False)

capture.download_all()

