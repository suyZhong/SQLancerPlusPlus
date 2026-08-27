import argparse
import os
import random
import sys
import time
from collections import defaultdict

# WebBaseLoader uses this value for outbound HTTP requests. Respect an explicit caller value while providing a
# descriptive default for local ShQveL runs.
os.environ.setdefault("USER_AGENT", "SQLancerPlusPlus-ShQveL/1.0 (+https://github.com/suyZhong/SQLancerPlusPlus)")

import yaml

from langchain_community.document_loaders import WebBaseLoader
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.chains.combine_documents import create_stuff_documents_chain

DBMS_MAPPING = defaultdict(lambda: "Unknown", {
    "duckdb" : "DuckDB",
    "postgresql" : "PostgreSQL",
    "postgres" : "PostgreSQL",
    "cedardb" : "CedarDB",
    "cratedb" : "CrateDB",
    "cockroachdb" : "CockroachDB",
    "sqlite" : "SQLite",
})

OVERWRITE = True
DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(DIR, "..", "dbconfigs")
YAML_DIR = CONFIG_DIR + "/url.yml"

PROMPTS = {
    "datatype_general" : [("system", "What are the data types in {DBMS} based on the documentation: \\n\\n {context}.\\n\\n Give me example values and definitions in CREATE TABLE statements")],
    "datatype_specific" : [("system", "What are {topic} for {DBMS} based on the documentation: \\n\\n {context}.\\n\\n Give me only names and examples. Please list all the possible values.")],
    "function_general" : [("system", "What are the functions in {DBMS} based on the documentation: \\n\\n {context}.\\n\\n Give me example usage and syntax.")],
    "function_specific" : [("system", "What are {topic} in {DBMS} based on the documentation: \\n\\n {context}.\\n\\n Give me as many example usage and syntax as possible.")],
    "all_specific" : [("system", "What are {topic} in {DBMS} based on the documentation: \\n\\n {context}.\\n\\n Give me example usage and syntax.")],
    "clause_specific" : [("system", "What are {topic} in {DBMS} based on the documentation: \\n\\n {context}.\\n\\n Give me examples usage, syntax and detailed keywords.")],
    "command_specific" : [("system", "What are {topic} in {DBMS} based on the documentation: \\n\\n {context}.\\n\\n Give me exampes.")],
}

def get_docs_by_URL(url: str):
    loader = WebBaseLoader(url)
    return loader.load()

def configure_google_search() -> bool:
    credentials = {
        "GOOGLE_API_KEY": "GOOGLE_API.txt",
        "GOOGLE_CSE_ID": "GOOGLE_CSE.txt",
    }
    for environment_variable, file_name in credentials.items():
        if os.getenv(environment_variable):
            continue
        credential_file = os.path.join(CONFIG_DIR, file_name)
        if os.path.isfile(credential_file):
            with open(credential_file) as credential:
                os.environ[environment_variable] = credential.read().strip()
    return all(os.getenv(environment_variable) for environment_variable in credentials)

def search_google_get_first_link(query: str):
    if not configure_google_search():
        raise RuntimeError(
            "No documentation URL is configured and Google search credentials are unavailable. "
            "Add an official documentation URL to dbconfigs/url.yml, or set GOOGLE_API_KEY and GOOGLE_CSE_ID."
        )

    from langchain_google_community import GoogleSearchAPIWrapper

    search = GoogleSearchAPIWrapper()
    print(f"Search result for {query}:")
    result = search.results(query, 5)
    return result[0]["link"]

def get_urls_from_yaml(dbms: str, feature: str, dir: str, topic: str = "") -> list:
    with open(dir) as f:
        url_yaml = yaml.safe_load(f) or {}

    updated = False
    # get the dict to the level of feature
    try:
        feature_dict = url_yaml[dbms][feature]
    except (KeyError, TypeError):
        url = search_google_get_first_link(f"{dbms} {feature} documentation")
        print(f"Automatically found the URL for {dbms} {feature} documentation: {url}")
        url_yaml.setdefault(dbms, {})[feature] = {"overview": [url]}
        feature_dict = url_yaml[dbms][feature]
        updated = True

    # get the fine-grained urls
    urls = []
    if topic == "" or topic == "overview":
        try:
            urls = feature_dict["overview"]
        except KeyError:
            raise ValueError(f"Please provide a valid feature. Available options are: {list(feature_dict.keys())}")
    else:
        try:
            urls = feature_dict[topic]
        except KeyError:
            if configure_google_search():
                url = search_google_get_first_link(f"{dbms} documentation for {topic}")
                print(f"Automatically found the URL for {dbms} documentation for {feature} {topic}: {url}")
                url_yaml[dbms][feature][topic] = [url]
                urls = [url]
                updated = True
            elif feature_dict.get("overview"):
                urls = feature_dict["overview"]
                print(
                    f"No URL configured for {dbms} {feature} {topic}; using the feature overview.",
                    file=sys.stderr,
                )
            else:
                raise RuntimeError(
                    f"No documentation URL configured for {dbms} {feature} {topic}. "
                    "Add one to dbconfigs/url.yml."
                )

    # update the yaml file
    if updated and OVERWRITE:
        with open(dir, "w") as f:
            yaml.dump(url_yaml, f)
    elif updated:
        backup_dir = CONFIG_DIR + "/urls"
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M")
        with open(f"{backup_dir}/url_{timestamp}.yml", "w") as f:
            yaml.dump(url_yaml, f)
    return urls


def learn_reference(docs, dbms: str, chain):
    result = chain.invoke({"context": docs, "DBMS": dbms})
    print(result)


def learn_reference_with_topic(docs, dbms: str, chain, topic: str):
    result = chain.invoke({"context": docs, "DBMS": dbms, "topic": topic})
    print(result)

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--model", type=str, default="gpt-4o-mini")
    argparser.add_argument("--dbms", type=str, default="")
    argparser.add_argument("--feature", type=str, default="", choices=["datatype", "function", "command", "clause"])
    argparser.add_argument("--topic", type=str, default="")
    argparser.add_argument("--learn", action="store_true")
    argparser.add_argument("--debug", action="store_true")
    argparser.add_argument("--yaml", type=str, default="")
    args = argparser.parse_args()

    avail_dbms = list(DBMS_MAPPING.keys())
    dbms = DBMS_MAPPING[args.dbms.lower()]
    if dbms == "Unknown":
        raise ValueError(f"Please provide an existing DBMS name. Available options are: {avail_dbms}")

    if args.topic == "overview":
        prompt_tag = f"{args.feature}_general"
    else:
        prompt_tag = f"{args.feature}_specific"

    if args.yaml != "":
        yaml_dir = CONFIG_DIR + "/" + args.yaml
    else:
        yaml_dir = YAML_DIR

    # Get the URL and the docs
    urls = get_urls_from_yaml(dbms, args.feature, yaml_dir, args.topic)
    url = random.choice(urls)
    docs = get_docs_by_URL(url)
    if args.debug:
        print(docs[0].page_content)

    if not args.learn:
        print("Skip to learn the reference.")
        exit()

    # Creating the chain
    llm = ChatOpenAI(model=args.model)
    default_prompt = [("system", "Summarize the {DBMS} documentation: \\n\\n {context}.\\n\\n Give me example values and definitions.")]
    prompts = defaultdict(lambda: default_prompt, PROMPTS)
    prompt = ChatPromptTemplate.from_messages(prompts[prompt_tag])
    chain = create_stuff_documents_chain(llm, prompt)

    # Learning the reference
    if prompt_tag.endswith("general"):
        learn_reference(docs, dbms, chain)
    elif prompt_tag.endswith("specific"):
        learn_reference_with_topic(docs, dbms, chain, args.topic)
