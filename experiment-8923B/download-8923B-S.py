from functions import *
import os

capture = Capture(working_dir=os.getcwd(),
        notebook_path="/Repos/Shared/spark-ui-simulator-experiments/Exp #8923B-S - Read Tiny Files w Photon",
        headless=True,
        version="v004-S",
        languages=["scala"],
        has_lab=False)

capture.download_all()

