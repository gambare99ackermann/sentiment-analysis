import os
import json
from time import sleep
from confluent_kafka import Consumer, Producer
from langchain.prompts import PromptTemplate
from langchain_groq import ChatGroq
from langchain.chat_models import ChatOpenAI
from dotenv import load_dotenv

# === Load environment variables ===
load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka.svc:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "by0bl57qw19qnewv91wg0wsig")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "GyB6AGkXzDDQr66pv9FnCVyNVDAwpSYr")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "cold-call-leads")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "cold-call-leads-enriched")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

SENTIMENT_LABELS = ["Positive", "Neutral / Follow-up Required", "Negative"]

# === LangChain LLMs ===
sentiment_llm = ChatGroq(
    model_name="llama3-8b-8192",
    temperature=0.3,
    max_tokens=300,
    groq_api_key=GROQ_API_KEY
)

action_llm = ChatOpenAI(
    model="mistralai/mistral-7b-instruct",
    openai_api_base="https://openrouter.ai/api/v1",
    openai_api_key=OPENROUTER_API_KEY,
    temperature=0.6
)

# === Prompt Templates ===
sentiment_prompt = PromptTemplate(
    input_variables=["notes", "sentiment"],
    template="""
Classify the following cold call notes into one of the following sentiment labels:
{sentiment}

Notes:
{notes}

Return only the label and a brief reason.
"""
)

action_prompt = PromptTemplate(
    input_variables=["notes", "sentiment"],
    template="""
You are a sales assistant for a Kafka-based platform called Condense.

The following are notes from a recent cold call:
"{notes}"

Sentiment label: {sentiment}

Based on this:
- If the sentiment label is "Positive":
  - Carefully read the caller notes and come up with the sales reachout plan step by step which should be customised based on caller notes. Write ready-to-consume reachout and sales plan.

- If the sentiment label is "Follow-up Required" or "Neutral":
  - Check the caller notes.
  - If there are signs of mild interest or open-ended comments, come up with the sales reachout plan step by step. Write ready-to-consume reachout and sales plan.

- If the sentiment label is "Negative":
  - Do not suggest any follow-up. Just say: "No further action recommended based on negative sentiment."

Respond clearly and helpfully.
"""
)

# === Chains ===
sentiment_chain = sentiment_prompt | sentiment_llm

# === Kafka Setup ===
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'sentiment-analyzer-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD
}
consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_INPUT_TOPIC])

producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD
})

# === Main Processor ===
def classify_and_generate(notes):
    try:
        label_list = ', '.join(SENTIMENT_LABELS)
        sentiment_response = sentiment_chain.invoke({"notes": notes, "sentiment": label_list})
        sentiment = sentiment_response.content.strip()
    except Exception as e:
        sentiment = f"❌ LLM failed (sentiment): {e}"
        return {
            "Caller Notes": notes,
            "Sentiment Label": "Unknown",
            "Sentiment Score": "-",
            "Reason": sentiment,
            "Next Action Items": "❌ Skipped due to classification error"
        }

    # Handle negative
    if "negative" in sentiment.lower():
        return {
            "Caller Notes": notes,
            "Sentiment Label": sentiment,
            "Sentiment Score": "-",
            "Reason": "Negative sentiment detected",
            "Next Action Items": "❌ No action recommended due to negative sentiment."
        }

    try:
        action_input = action_prompt.format(notes=notes, sentiment=sentiment)
        action_items = action_llm.invoke(action_input).content.strip()
    except Exception as e:
        action_items = f"❌ LLM failed (action): {e}"

    return {
        "Caller Notes": notes,
        "Sentiment Label": sentiment,
        "Sentiment Score": "-",
        "Reason": "Sentiment identified by Groq",
        "Next Action Items": action_items
    }

# === Kafka Consume-Produce Loop ===
print(f"📡 Listening to Kafka topic: {KAFKA_INPUT_TOPIC}")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Consumer error: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode('utf-8'))
            comments = payload.get("comments", "")
            result = classify_and_generate(comments)
            payload['result'] = result
            enriched_data = json.dumps(payload).encode('utf-8')
            producer.produce(KAFKA_OUTPUT_TOPIC, enriched_data)
            producer.flush()
            print(f"✅ Published enriched message: {result['Sentiment Label']}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")

        sleep(0.5)
except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")
finally:
    consumer.close()
