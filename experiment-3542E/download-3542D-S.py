from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Repos/Shared/spark-ui-simulator-experiments/Exp #3542 - Join/Exp #3542A-S - Simple Join - Photon",
        headless=True,
        version="v004-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()

