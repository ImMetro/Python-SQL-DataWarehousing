import time
import pyautogui

def play_recorded_mouse_activity(mouse_activity, speed=1.0, delay_between_actions=3):
    print("Playing back recorded mouse activity...")
    
    for i in range(0,2000):
        time.sleep(5)  # Give a 2-second pause before starting

        for x, y in mouse_activity:
            pyautogui.moveTo(x, y, duration=0.0)  # Move the cursor to (x, y)
            pyautogui.click(button='left')  # Simulate a left mouse click

            time.sleep(0.01 / speed)  # Adjust the playback speed if needed
            time.sleep(delay_between_actions)

        print(f"Successfully exported {i} reports so far.. Total is 2000")
    print("Playback complete.")

if __name__ == "__main__":
    # Replace 'recorded_activity.txt' with the path to your recorded mouse activity file
    with open('action.txt', 'r') as file:
        lines = file.readlines()

    mouse_activity = [tuple(map(int, line.strip().split(', '))) for line in lines]
    playback_speed = 1.0  # You can adjust the playback speed (1.0 is the original speed)
    action_delay=5

    play_recorded_mouse_activity(mouse_activity, speed=playback_speed,delay_between_actions=action_delay)
