from bedrock_agentcore.runtime import BedrockAgentCoreApp
from orchestration import run_pipeline

app = BedrockAgentCoreApp()

@app.entrypoint
def invoke(payload: dict, context=None):

    input_text = (
        payload.get("input_text")
        or payload.get("message")
        or payload.get("question")
        or payload.get("prompt")
        or ""
    )

    if not input_text:
        return {"status": "error", "message": "No input_text provided"}

    try:
        result = run_pipeline(input_text)
        return result if isinstance(result, dict) else {"result": result}

    except Exception as e:
        return {"status": "error", "message": str(e)}

app.run()