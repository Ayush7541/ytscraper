from openai import OpenAI

# Replace with your key (or use env variable)
api_key = "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"
client = OpenAI(api_key=api_key)

try:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "Say hello in a friendly way."}],
        temperature=0
    )
    print("API is working! Response:")
    print(response.choices[0].message.content)
except Exception as e:
    print("API test failed:", e)