#!/usr/bin/env -S /home/fan_of_luna/.local/bin/uv run --script

from app import app


if __name__ == "__main__":
    app.run(host="localhost", debug=True)
