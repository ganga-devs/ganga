import webbrowser
import pyautogui
import time
webbrowser.open_new_tab("http://localhost:8080/index.html")
time.sleep(20)
pyautogui.write('ganga-gui & disown')
pyautogui.press('enter')
