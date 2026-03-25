import os
from urllib.parse import urlparse
import boto3
from dotenv import load_dotenv
from opensearchpy import OpenSearch, RequestsAWSV4SignerAuth, RequestsHttpConnection

from planner import Planner
from normalizer import Normalizer
from structured_retriever import StructuredRetriever
from vector_retriever import VectorRetriever
from fusion import FusionLayer
from synthesis import ContextBuilder, Synthesizer, Validator

load_dotenv()

def main() -> None:
    planner = Planner()
    aliases_path = "aliases_reviewed.json" if os.path.exists("aliases_reviewed.json") else "aliases.json"
    normalizer = Normalizer(aliases_path)
    os_host = os.getenv("OPENSEARCH_HOST", "http://localhost:9200")
    parsed = urlparse(os_host)
    is_aws = parsed.scheme == "https"
    os_kwargs: dict = {
        "hosts": [{"host": parsed.hostname, "port": parsed.port or (443 if is_aws else 9200)}],
        "http_compress": True,
        "use_ssl": is_aws,
        "verify_certs": is_aws,
    }
    if is_aws:
        credentials = boto3.Session().get_credentials()
        region = os.getenv("AWS_REGION", "eu-central-1")
        os_kwargs["http_auth"] = RequestsAWSV4SignerAuth(credentials, region, "es")
        os_kwargs["connection_class"] = RequestsHttpConnection
    os_client = OpenSearch(**os_kwargs)
    structured_retriever = StructuredRetriever(os_client=os_client)
    vector_retriever = VectorRetriever(os_client=os_client)
    fusion_layer = FusionLayer()
    context_builder = ContextBuilder()
    synthesizer = Synthesizer()
    validator = Validator()

    while True:
        try:
            user_query = input("\n>>> ").strip()
        except EOFError:
            break

        if user_query.lower() in ("exit", "quit"):
            break

        plan_obj = planner.plan(user_query)
        print(f"plan: mode={plan_obj.mode} intent={plan_obj.intent} filters={len(plan_obj.filters)}")

        plan_obj_norm = normalizer.normalize(plan_obj)
        print(f"normalize: {len(plan_obj_norm.filters)} filter(s) after normalization")

        # Retrieval
        structured_result = None
        vector_result = None

        if plan_obj_norm.mode == "structured":
            structured_result = structured_retriever.retrieve(plan_obj_norm)

        elif plan_obj_norm.mode == "vector":
            vector_result = vector_retriever.retrieve(plan_obj_norm)
            structured_result = structured_retriever.fetch(vector_result.candidate_project_ids)

        elif plan_obj_norm.mode == "hybrid":
            has_filters = bool(plan_obj_norm.filters)

            if not has_filters:
                # Branch A: semantic-driven
                vector_result = vector_retriever.retrieve(plan_obj_norm)
                structured_result = structured_retriever.fetch(vector_result.candidate_project_ids)
            else:
                # Branch B: filter-driven
                structured_result = structured_retriever.retrieve(plan_obj_norm)
                vector_result = vector_retriever.retrieve(plan_obj_norm)
                # Backfill ProjectCards for projects VectorRetriever discovered
                new_ids = [
                    pid for pid in vector_result.candidate_project_ids
                    if pid not in set(structured_result.candidate_project_ids)
                ]
                if new_ids:
                    backfill = structured_retriever.fetch(new_ids)
                    structured_result.projects.extend(backfill.projects)
                    structured_result.candidate_project_ids.extend(backfill.candidate_project_ids)

        if structured_result is not None:
            print(f"structured: projects={len(structured_result.projects)}")

        if vector_result is not None:
            print(f"vector: chunks={len(vector_result.chunks)}")

        # Fusion
        fused = fusion_layer.fuse(plan_obj_norm, structured_result, vector_result)
        print(f"fusion: projects={len(fused.projects)} total={fused.total} warnings={len(fused.warnings)}")

        # Context
        ctx = context_builder.build(plan=plan_obj_norm, fused=fused)
        print(f"context: system={len(ctx.system)} chars, user={len(ctx.user)} chars")
        print(f"\n{ctx.user}")

        # Synthesis
        answer = synthesizer.synthesize(ctx)
        print(f"\nsynthesis: {len(answer)} chars")
        print(f"{answer}")

        # Validation
        validated = validator.validate(answer=answer, fused=fused)
        print(f"\nvalidation: citations={len(validated.citations)} warnings={len(validated.warnings)}")
        print(f"{validated.warnings}")


if __name__ == "__main__":
    main()
