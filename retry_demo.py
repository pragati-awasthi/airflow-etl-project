import time

attempt_success = 3   # Change number to decide when it should succeed

for attempt in range(1, 6):
    print(f"Attempt {attempt}...")

    if attempt < attempt_success:
        print("Temporary failure... retrying!\n")
        time.sleep(1)   # wait 1 second before retry
    else:
        print(" SUCCESS on retry!")
        break
else:
    print(" Escalation: All retries failed!")

