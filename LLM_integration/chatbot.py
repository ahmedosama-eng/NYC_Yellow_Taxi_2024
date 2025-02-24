import sys
import os
import openai
from langchain_community.document_loaders.csv_loader import CSVLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationalRetrievalChain
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from LLM_integration.Embadding import documents_converter,text_spliter,Embeddings_and_vectordb
from LLM_integration.data_ingestion import prepare_for_embadding


sys.path.append('../..')

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv()) 

openai.api_key  = os.environ['OPENAI_API_KEY']



def get_completion(prompt, model="gpt-3.5-turbo"): 
    messages = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0, 
    )
    return response.choices[0].message["content"]




def chatbot_model(docsearch):
    llm = ChatOpenAI(model_name='gpt-3.5-turbo', temperature=1)
    chat_memory = ConversationBufferMemory(
        memory_key="chat_history",
        return_messages=True
    )
    qa = ConversationalRetrievalChain.from_llm(
        llm,
        retriever=docsearch.as_retriever(),
        memory=chat_memory
    )
    return qa



def chatbot(data,vector_db_path,cheak_path):
    db_vector=Embeddings_and_vectordb(data,vector_db_path,cheak_path)
    print("here we chatting")
    chatbot=chatbot_model(db_vector)
    return chatbot

