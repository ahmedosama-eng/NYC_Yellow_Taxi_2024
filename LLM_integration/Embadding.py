from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
import os
from LLM_integration.data_ingestion import prepare_for_embadding
from utils import check_file_exists


def documents_converter(data_dict):
    print("here we convert to doc")
    documents = []
    
    for filename, df in data_dict.items():
            print(filename)
            text_rows = []
            for row in df.toLocalIterator():  
                text_rows.append(", ".join(map(str, row)))
                if len(text_rows) >= 1000:  
                    documents.append(Document(page_content="\n".join(text_rows), metadata={"source": filename}))
                    text_rows = []  
            
            
            if text_rows:
                documents.append(Document(page_content="\n".join(text_rows), metadata={"source": filename}))

    return documents


def text_spliter(data):           
    print("here we splite to chunks")
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=20)
    text_chunks = text_splitter.split_documents(data)
    return text_chunks


def Embeddings_and_vectordb(data,vdb_path,cheak_path):
    # cheak of its already embadded
    if check_file_exists(cheak_path)==True:
        docsearch = FAISS.load_local(vdb_path,embeddings=OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY")),allow_dangerous_deserialization=True)
    else :
        data_dictionary=prepare_for_embadding(data)
        documents=documents_converter(data_dictionary)
        chunks=text_spliter(documents)
        print("here we embedding")
        embedding = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
        docsearch = FAISS.from_documents(chunks, embedding)
        docsearch.save_local(vdb_path)
    return docsearch


 