import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
load_dotenv()  
import os

fake = Faker()


CHANNEL_MAPPING = [
    {'channel_id': 1, 'channel': 'Online Payment', 'description': 'E-commerce', 'base_rate': 0.25, 'percentage_fee': 0.015, 'per_minute': 0.00},
    {'channel_id': 2, 'channel': 'TPE', 'description': 'Card Present', 'base_rate': 0.10, 'percentage_fee': 0.010, 'per_minute': 0.00},
    {'channel_id': 3, 'channel': 'ATM', 'description': 'Cash Withdrawal', 'base_rate': 2.50, 'percentage_fee': 0.000, 'per_minute': 0.00},
    {'channel_id': 4, 'channel': 'Mobile App', 'description': 'P2P Transfer', 'base_rate': 0.00, 'percentage_fee': 0.000, 'per_minute': 0.00},
    {'channel_id': 5, 'channel': 'Branch', 'description': 'In-person', 'base_rate': 1.00, 'percentage_fee': 0.005, 'per_minute': 0.00}
]

PAYMENT_METHOD_MAPPING = [
    {'payment_method_id': 1, 'payment_method': 'Credit Card', 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 2, 'payment_method': 'Debit Card', 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 3, 'payment_method': 'Bank Transfer', 'is_card': False, 'requires_auth': False},
    {'payment_method_id': 4, 'payment_method': 'Direct Debit', 'is_card': False, 'requires_auth': False}
]

TRANSACTION_STATUS_MAPPING = [
    {'transaction_status_id': 1, 'transaction_status': 'Approved', 'is_completed': True},
    {'transaction_status_id': 2, 'transaction_status': 'Declined', 'is_completed': False}
]

CARD_NETWORK_MAPPING = [
    {'card_network_id': 1, 'card_network': 'Visa'},
    {'card_network_id': 2, 'card_network': 'Mastercard'},
    {'card_network_id': 3, 'card_network': 'Amex'},
    {'card_network_id': 4, 'card_network': 'UnionPay'},
    {'card_network_id': 5, 'card_network': 'Discover'},
    {'card_network_id': 6, 'card_network': 'JCB'},
    {'card_network_id': 7, 'card_network': 'Diners Club'}
]

CARD_NETWORKS_LIST = [m['card_network'] for m in CARD_NETWORK_MAPPING]
CARD_NETWORK_ID_MAP = {m['card_network']: m['card_network_id'] for m in CARD_NETWORK_MAPPING}

CHANNELS_LIST = [t['channel'] for t in CHANNEL_MAPPING]
CHANNEL_ID_MAP = {t['channel']: t['channel_id'] for t in CHANNEL_MAPPING}

PAYMENT_METHODS_LIST = [p['payment_method'] for p in PAYMENT_METHOD_MAPPING]
PAYMENT_METHOD_ID_MAP = {p['payment_method']: p['payment_method_id'] for p in PAYMENT_METHOD_MAPPING}

TRANSACTION_STATUSES_LIST = [s['transaction_status'] for s in TRANSACTION_STATUS_MAPPING]
TRANSACTION_STATUS_ID_MAP = {s['transaction_status']: s['transaction_status_id'] for s in TRANSACTION_STATUS_MAPPING}


BRANCH_MAPPING = [
    {'branch_id': 1, 'branch': 'New York Main', 'city': 'New York', 'region': 'Northeast'},
    {'branch_id': 2, 'branch': 'Los Angeles Downtown', 'city': 'Los Angeles', 'region': 'West'},
    {'branch_id': 3, 'branch': 'Chicago Loop', 'city': 'Chicago', 'region': 'Midwest'},
    {'branch_id': 4, 'branch': 'Houston Galleria', 'city': 'Houston', 'region': 'South'},
    {'branch_id': 5, 'branch': 'Phoenix Central', 'city': 'Phoenix', 'region': 'Southwest'},
    {'branch_id': 6, 'branch': 'Philadelphia Center', 'city': 'Philadelphia', 'region': 'Northeast'},
    {'branch_id': 7, 'branch': 'San Antonio Riverwalk', 'city': 'San Antonio', 'region': 'South'},
    {'branch_id': 8, 'branch': 'San Diego Bay', 'city': 'San Diego', 'region': 'West'},
    {'branch_id': 9, 'branch': 'Dallas Uptown', 'city': 'Dallas', 'region': 'South'},
    {'branch_id': 10, 'branch': 'San Jose Tech', 'city': 'San Jose', 'region': 'West'}
]

BRANCH_LIST = [c['branch'] for c in BRANCH_MAPPING]
BRANCH_ID_MAP = {c['branch']: c['branch_id'] for c in BRANCH_MAPPING}

DECLINE_REASON_MAPPING = [
    {'decline_reason_id': 1, 'decline_reason': 'Insufficient Funds'},
    {'decline_reason_id': 2, 'decline_reason': 'Fraud Suspected'},
    {'decline_reason_id': 3, 'decline_reason': 'Limit Exceeded'},
    {'decline_reason_id': 4, 'decline_reason': None}  # Approved transactions
]

DECLINE_REASON_ID_MAP = {c['decline_reason']: c['decline_reason_id'] for c in DECLINE_REASON_MAPPING}



def generate_banking_transaction():
    
    # Generate timestamps
    initiation_time = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))

    
    processing_duration = random.randint(1, 10) 
    settlement_time = initiation_time + timedelta(seconds=processing_duration)
    booking_time = initiation_time - timedelta(seconds=random.randint(1, 10))
    

    transaction_amount = round(random.uniform(0.5, 50), 2)
    

    channel = random.choice(CHANNELS_LIST)
    channel_id = CHANNEL_ID_MAP[channel]

    
    channel_details = next(c for c in CHANNEL_MAPPING if c['channel_id'] == channel_id)
    base_fee = channel_details['base_rate']
    percentage_rate = channel_details['percentage_fee']
    risk_multiplier = round(random.uniform(1.0, 1.5), 2) 
    
    variable_fee = round(transaction_amount * percentage_rate, 2)
    processing_fee = 0.0 
    subtotal = round((variable_fee + base_fee) * risk_multiplier, 2)
    
    
    total_amount = round(subtotal, 2)
    
    
    customer_address = fake.address().replace('\n', ', ')
    payee_address = fake.address().replace('\n', ', ')
    
    
    customer_branch = random.choice(BRANCH_LIST)
    payee_branch = random.choice(BRANCH_LIST)
    customer_branch_id = BRANCH_ID_MAP[customer_branch]
    payee_branch_id = BRANCH_ID_MAP[payee_branch]
    
    
    card_network = random.choice(CARD_NETWORKS_LIST)
    card_network_id = CARD_NETWORK_ID_MAP[card_network]
    
    
    is_declined = random.random() < 0.1
    decline_reason = None
    decline_reason_id = 4  # Default: None (Approved)
    if is_declined:
        decline_reason = random.choice(['Insufficient Funds', 'Fraud Suspected', 'Limit Exceeded'])
        decline_reason_id = DECLINE_REASON_ID_MAP[decline_reason]

    
    payment_method = random.choice(PAYMENT_METHODS_LIST)
    payment_method_id = PAYMENT_METHOD_ID_MAP[payment_method]

    
    # Derive status FROM the decline decision
    if is_declined:
        transaction_status_id = 2  # Declined
    else:
        transaction_status_id = 1  # Approved
    
    
    transaction_confirmation = {
        # Keys/Identifiers
        'transaction_id': str(uuid.uuid4()),
        'transaction_reference': fake.bothify('??#-####-??##').upper(),
        'customer_id': str(uuid.uuid4()),
        'payee_id': str(uuid.uuid4()),
        'account_id': str(uuid.uuid4()),
        'source_location_id': str(uuid.uuid4()),
        'destination_location_id': str(uuid.uuid4()),
        
        # Foreign Keys to Mapping Tables
        'channel_id': channel_id,
        'card_network_id': card_network_id,
        'payment_method_id': payment_method_id,
        'transaction_status_id': transaction_status_id,
        'branch_id': customer_branch_id,
        'beneficiary_branch_id': payee_branch_id,
        'decline_reason_id': decline_reason_id,
        
        # Customer Information 
        'customer_name': fake.name(),
        'customer_email': fake.email(),
        'customer_phone': fake.numerify('###-###-####'),
        
        # Payee Information
        'payee_name': fake.name(),
        
        'payee_risk_score': random.randint(1, 100), 
        'payee_phone': fake.numerify('###-###-####'),
        'payee_tax_id': fake.numerify('##-#######'),  
        
        # Account Information 
        'account_type': random.choice(['Checking', 'Savings', 'Credit', 'Business', 'Student']),
        'card_tier': random.choice(['Classic', 'Gold', 'Platinum', 'Silver', 'Business', 'Student']),
        'account_number': fake.numerify('####-####'),
        
        # Customer & Payee Locations 
        'customer_address': customer_address,
        'customer_latitude': round(random.uniform(25.0, 49.0), 6),
        'customer_longitude': round(random.uniform(-125.0, -66.0), 6),
        'payee_address': payee_address,
        'payee_latitude': round(random.uniform(25.0, 49.0), 6),
        'payee_longitude': round(random.uniform(-125.0, -66.0), 6),
        
        # Transaction Details
        'transaction_amount': transaction_amount,
        'processing_duration': processing_duration,
        'initiation_timestamp': booking_time.isoformat(),
        'transaction_timestamp': initiation_time.isoformat(),
        'settlement_timestamp': settlement_time.isoformat(),
        
        # Pricing
        'base_fee': base_fee,
        'variable_fee': variable_fee,
        'processing_fee': processing_fee,
        'risk_multiplier': risk_multiplier,
        'subtotal': subtotal,
        'total_amount': total_amount,
        
        # Payment & Status
        'feedback_score': random.choice([None, random.randint(1, 5)])
    }
    
    return transaction_confirmation