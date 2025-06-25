import requests, json, time

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL     = "http://localhost:8000"
LEADS_URL    = f"{BASE_URL}/api/v1/leads"
PAY_URL      = f"{BASE_URL}/api/v1/payments"

VALID_TOKEN   = "eyJ...VALID..."    # replace with your valid JWT
INVALID_TOKEN = "bad.token"
EXPIRED_TOKEN = "eyJ...EXPIRED..."  # a token you know is expired

HEADERS = lambda token=None: {
    **({"Authorization": f"Bearer {token}"} if token else {}),
    "Content-Type": "application/json"
}

# Sample “happy path” payloads:
VALID_LEAD = {
    "leads": [
        {
            "date": "2025-06-01 - 2025-06-30",
            "realid": "OC999",
            "name": "Test User",
            "phone_1": 9123456789,
            "phone_2": None,
            "email": "test.user@example.com",
            "gender": "female",
            "preferred_language": "en",
            "home_state": "MH",
            "segment_band": "5.B0_Segment5",
            "collection_stage": "predue",
            "emi_eligible_flag": True,
            "priority": 5,
            "bill_date": "2025-06-01",
            "due_date": "2025-06-15",
            "total_due": 1000,
            "min_due": 100,
            "any_dispute_raised": None,
            "days_past_due": 0,
            "app_lastvisit_timestamp_after_bill_date": None,
            "app_payment_visit": None,
            "last_connected_call_time": None,
            "last_payment_details": None,
            "last_connected_conversation": None
        }
    ]
}

VALID_PAY = {
    "payments": [
        {"date": "2025-06-01 - 2025-06-30", "realid": "OC999", "Amount": 1200, "Payment Tag": "MAD"},
        {"date": "2025-06-01 - 2025-06-30", "realid": "OC998", "Amount": 1,    "Payment Tag": "<STAB"}
    ]
}

# ──────────────────────────────────────────────────────────────────────────────
# TEST MATRIX DEFINITION
# ──────────────────────────────────────────────────────────────────────────────

TESTS = [

    # LEADS — Authentication
    ("TC-L001", "leads", VALID_LEAD, VALID_TOKEN,    [200,201], None),
    ("TC-L002", "leads", VALID_LEAD, None,           [401],    "Authentication required"),
    ("TC-L003", "leads", VALID_LEAD, INVALID_TOKEN,  [401],    "Invalid token"),
    ("TC-L004", "leads", VALID_LEAD, EXPIRED_TOKEN,  [401],    "Token expired"),

    # LEADS — Data Validation
    ("TC-L005", "leads", VALID_LEAD, VALID_TOKEN,   [200,201], None),
    ("TC-L006", "leads", {**VALID_LEAD, "leads": VALID_LEAD["leads"]*2}, VALID_TOKEN, [200,201], None),
    ("TC-L007", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"realid":None}}]}, VALID_TOKEN, [400], "realid is required"),
    ("TC-L008", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"name":None}}]}, VALID_TOKEN, [400], "name is required"),
    ("TC-L009", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"phone_1":"1234"}}]}, VALID_TOKEN, [400], "phone_1 must be integer"),
    ("TC-L010", "leads", {"leads":[{**VALID_LEAD["leads"][0].copy(), **{"date":None}}]}, VALID_TOKEN, [400], "date is required"),
    ("TC-L011", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"total_due":"500"}}]}, VALID_TOKEN, [400], "total_due must be integer"),
    ("TC-L012", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"emi_eligible_flag":"true"}}]}, VALID_TOKEN, [400], "emi_eligible_flag must be boolean"),
    ("TC-L013", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"bill_date":"01-06-2025"}}]}, VALID_TOKEN, [400], "Invalid date format"),
    ("TC-L014", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"gender":"other"}}]}, VALID_TOKEN, [400], "gender must be"),
    ("TC-L015", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"preferred_language":"english"}}]}, VALID_TOKEN, [400], "Invalid language code"),
    ("TC-L016", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"segment_band":"X"}}]}, VALID_TOKEN, [400], "Invalid segment_band"),
    ("TC-L017", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"phone_1":1234567}}]}, VALID_TOKEN, [400], "phone_1 must be 10 digits"),
    ("TC-L018", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"email":"bad-email"}}]}, VALID_TOKEN, [400], "Invalid email format"),

    # LEADS — Business & Error handling
    ("TC-L019", "leads", VALID_LEAD, VALID_TOKEN, [200,201], None),
    ("TC-L020", "leads", VALID_LEAD, VALID_TOKEN, [409],    "Duplicate realid"),
    ("TC-L021", "leads", {"leads":[{**VALID_LEAD["leads"][0], **{"phone_2":None, "email":None}}]}, VALID_TOKEN, [200,201], None),
    ("TC-L022", "leads", '{"leads":[{"name":"x"}', VALID_TOKEN, [400], "Invalid JSON"),
    ("TC-L023", "leads", {}, VALID_TOKEN, [400], "cannot be empty"),

    # PAYMENTS — Authentication
    ("TC-P001", "payments", VALID_PAY,   VALID_TOKEN, [200,201], None),
    ("TC-P002", "payments", VALID_PAY,   None,        [401],    "Authentication required"),
    ("TC-P003", "payments", VALID_PAY,   INVALID_TOKEN,[401],    "Invalid token"),
    ("TC-P004", "payments", VALID_PAY,   EXPIRED_TOKEN,[401],    "Token expired"),

    # PAYMENTS — Data validation
    ("TC-P005", "payments", VALID_PAY, VALID_TOKEN, [200,201], None),
    ("TC-P006", "payments", {"payments":[{}]}, VALID_TOKEN, [400], "realid is required"),
    ("TC-P007", "payments", {"payments":[{"realid":"x"}]}, VALID_TOKEN, [400], "Amount is required"),
    ("TC-P008", "payments", {"payments":[{"realid":"x","Amount":"100"}]}, VALID_TOKEN, [400], "must be integer"),
    ("TC-P009", "payments", {"payments":[{"realid":"x","Amount":-1,"Payment Tag":"MAD"}]}, VALID_TOKEN, [400], "must be positive"),
    ("TC-P010", "payments", {"payments":[{"realid":"x","Amount":0,"Payment Tag":"MAD"}]}, VALID_TOKEN, [400], "greater than 0"),
    ("TC-P011", "payments", VALID_PAY, VALID_TOKEN, [200,201], None),
    ("TC-P012", "payments", VALID_PAY, VALID_TOKEN, [409],    "Duplicate payment"),

    # PAYMENTS — Malformed/empty
    ("TC-P013", "payments", '{"payments":[{"Amount":1000}}', VALID_TOKEN, [400], "Invalid JSON"),
    ("TC-P014", "payments", {}, VALID_TOKEN, [400], "cannot be empty"),
]

# ──────────────────────────────────────────────────────────────────────────────
# RUNNER
# ──────────────────────────────────────────────────────────────────────────────

def send(method, url, body, headers):
    if isinstance(body, str):
        # invalid JSON test: send raw string
        return requests.request(method, url, data=body, headers=headers)
    return requests.request(method, url, json=body, headers=headers)

def run_test(test_id, ep_type, body, token, expected_codes, expect_msg):
    url = LEADS_URL if ep_type=="leads" else PAY_URL
    resp = send("POST", url, body, HEADERS(token))
    code_ok = resp.status_code in expected_codes
    msg_ok  = (expect_msg is None) or (expect_msg.lower() in resp.text.lower())
    status  = "PASS" if (code_ok and msg_ok) else "FAIL"
    print(f"{test_id:8s} {status} {ep_type:8s} [{resp.status_code}]"
          f"{' – '+expect_msg if expect_msg else ''}")

if __name__=="__main__":
    print("Starting full API test suite…\n")
    for tc in TESTS:
        run_test(*tc)
        time.sleep(0.1)  # throttle
    print("\nDone.")
