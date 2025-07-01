
# Automating Tasks For Nick


This repo contains all the necessary scripts and resources which are used in automating tasks for Nick Pintozzi.

## Description

This repository automates daily sales reporting for a remote sales team using Calendly, Zoom, Google Sheets, and Slack.

The pipeline does the following:

- Pulls booked and conducted appointments from Calendly & Zoom.
- Parses sales data from a Google Sheet (Commission Tracker).
- Calculates 12 performance metrics including revenue, close rate, and show rate.
- Sends formatted daily reports to Slack automatically.

---

## Prerequisites & Dependencies

- `python3`
- Google Sheets API access
- Zoom OAuth app credentials
- Calendly API key
- Slack Bot Token

---

## Getting Started

### 1. Cloning the repo:

```bash
git clone https://github.com/<your-username>/daily-sales-metrics.git
cd daily-sales-metrics
```

### 2. Installing dependencies

```bash
pip install -r requirements.txt
```

### 3. Setting up environment variables

Create a `.env` file and add all required tokens and credentials.

### 4. Running the script

```bash
python main.py
```

---

## Sample Slack Output

```
✅ TOTAL APPOINTMENTS BOOKED
Date: 2025-07-01
• Mikaela: 13
• Mike: 9
• Sierra: 14
TOTAL BOOKED: 36
```

---

> Automates all 12 metrics. Supports weekends. No manual counting needed.
