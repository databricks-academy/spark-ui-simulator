from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Repos/Shared/spark-ui-simulator-experiments/Exp #2755 - P - Unhandled Skew",
        headless=True,
        version="v005-P",
        languages=["python"],
        has_lab=False)

capture.download_all()