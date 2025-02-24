from Transform.data_transformer import Transform_Cleaning_and_Marge_LLM
from Insights_Analysis.EDA import aggregate_Taxi
from LLM_integration.chatbot import chatbot
from flask import Flask, request,jsonify
from config import data,lookups,spark


app = Flask(__name__)



vector_db_path=r'F:\data project\Taxi_track_2024\LLM_integration\vector_db'
cheak_path=r"F:\data project\Taxi_track_2024\LLM_integration\vector_db\index.faiss"

chatting=None

def pipline_for_LLM(data,lookups,spark):
    global chatting
    ready_data=Transform_Cleaning_and_Marge_LLM(data,lookups,spark)
    chatting=chatbot(ready_data,vector_db_path,cheak_path)
    



@app.route('/NYC_Txai_2024', methods=['POST'])
def NYC_Txai_2024_chat():
   try: 
        request_data = request.get_json()
        user_query = request_data.get("message")
        response = chatting(user_query)
        return {"response": response['answer']}
   except Exception as e:
        return {"error": str(e)}, 500
   

if __name__ == '__main__':
    pipline_for_LLM(data,lookups,spark)
    app.run(host='0.0.0.0', port=6000)