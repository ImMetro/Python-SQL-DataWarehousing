import time
import clipboard
from pynput import mouse

def on_click(x, y, button, pressed):
    if pressed:
        mouse_activity.append((x, y))

def record_mouse_activity(duration):
    print("Recording mouse activity for {} seconds...".format(duration))

    global mouse_activity
    mouse_activity = []

    with mouse.Listener(on_click=on_click) as listener:
        start_time = time.time()

        while time.time() - start_time < duration:
            time.sleep(0.01)  # Adjust the delay if needed

    print("Recording complete.")
    return mouse_activity

def copy_mouse_activity_to_clipboard(mouse_activity):
    activity_string = '\n'.join(['{}, {}'.format(x, y) for x, y in mouse_activity])
    clipboard.copy(activity_string)
    print("Mouse activity copied to clipboard.")

if __name__ == "__main__":
    duration = 20  # You can set the duration (in seconds) for recording mouse activity
    mouse_activity = record_mouse_activity(duration)
    copy_mouse_activity_to_clipboard(mouse_activity)