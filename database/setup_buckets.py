from database import make_buckets

if __name__ == "__main__":
    try:
        make_buckets()
        print("Initial buckets made successfully")
    except Exception as e:
        print(f"Couldn't make buckets: {e}")
