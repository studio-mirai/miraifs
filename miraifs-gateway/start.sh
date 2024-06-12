#!/bin/bash

uvicorn miraifs_gateway.main:app --port 8000 --reload
