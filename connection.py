import json
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import os
from data import generate_banking_transaction

load_dotenv() 

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUBNAME = os.getenv("EVENT_HUBNAME")


def send_to_event_hub(transaction_data=None, batch_size=1):

    try:
        # Initialize Event Hub Producer Client
        producer = EventHubProducerClient.from_connection_string(
            CONNECTION_STRING,
            eventhub_name=EVENT_HUBNAME
        )
        
        # Prepare transaction records
        transaction_json = json.dumps(transaction_data) 
        
        # Create batch of events
        event_batch = producer.create_batch()

            
        # Create event with transaction data 
        event = EventData(transaction_json)
            
        # Add event to batch
        event_batch.add(event)

        # Send batch to Event Hub
        producer.send_batch(event_batch)
        
        producer.close()

        return "Successfully sent to Event Hub"
        
    except Exception as e:
        print(f"Error sending data to Event Hub: {str(e)}")
        return False



#just for testing the function
if __name__ == "__main__":
    
    print("=" * 80)
    print("SINGLE TRANSACTION CONFIRMATION")
    print("=" * 80)
    transaction = generate_banking_transaction()
    print(json.dumps(transaction, indent=2))

    
    print("\n" + "=" * 80)
    print("SENDING SINGLE TRANSACTION TO EVENT HUB")
    result = send_to_event_hub(transaction)
    print(f"Single transaction sent to Event Hub: {result}")