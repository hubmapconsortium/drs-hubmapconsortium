#!/bin/bash
gunicorn -w 8 -b 0.0.0.0:5000 'app:create_app()'

