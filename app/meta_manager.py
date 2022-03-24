# Meta Manager core logics

import sys


class MetaManager:

    target_directory: str = ''

    # def __init__(self):
    #     super().__init__()
    #     # SETUP MAIN WINDOw
    #     # Load widgets from "gui\uis\main_window\ui_main.py"
    #     # ///////////////////////////////////////////////////////////////
    #     self.ui = UI_MainWindow()
    #     self.ui.setup_ui(self)

    # SELECT TARGET DIRECTORY
    # ///////////////////////////////////////////////////////////////
    # @classmethod
    # def select_folder(cls, window):
        
    #     # FOLDER DIALOG
    #     dialog_folder = QFileDialog.getExistingDirectory(parent=window, caption="Select Directory")

    #     # 
    #     cls.target_directory = dialog_folder
    #     print(cls.target_directory)

    # @classmethod
    # def view_folder(cls):
    #     print(cls.target_directory)
