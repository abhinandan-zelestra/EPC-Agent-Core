import boto3

bedrock_agent_runtime = boto3.client("bedrock-agent-runtime")

KB_ID = "KKE3DDBSQG"

def retrieve_kb_context(query: str):

    response = bedrock_agent_runtime.retrieve(
        knowledgeBaseId=KB_ID,
        retrievalQuery={"text": query}
    )

    kb_text = ""

    for result in response["retrievalResults"]:
        kb_text += result["content"]["text"] + "\n"

    return kb_text