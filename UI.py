import streamlit as st
import requests

# Custom CSS for additional styling with background image
page_bg_image="""
<style>
[data-testid="stApp"]{
  background-image: url('https://w0.peakpx.com/wallpaper/1004/852/HD-wallpaper-taxi-cab-night-car.jpg');
  background-size: cover;
   
}

.main {
        background-color: rgba(0, 0, 0, 0.5); /* White background with some transparency */
    }
 .chatbot-response {
        background-color: #3d2c0d;
        border-radius: 10px;
        padding: 10px;
        margin-top: 10px;
        font-size: 16px;
    }

    .error-message {
        background-color: #B43F3F;
        border-radius: 10px;
        padding: 10px;
        margin-top: 10px;
        font-size: 16px;
    }

     .title {
        color: #0f0303;
        font-family: '28 Days Later' ;
        text-align: center;
    }




</style>
"""

st.markdown(
   page_bg_image,
    unsafe_allow_html=True
)
 
def main():
    st.markdown("<h1 class='title' style='color: black; font-weight: bold; text-shadow: -1px -1px 0 #fff, 1px -1px 0 #fff, -1px 1px 0 #fff, 1px 1px 0 #fff;'>NYC TAXI Chatbot</h1>", unsafe_allow_html=True)
    user_input = st.text_input("Enter your message:")
    if st.button("Send"):
        if user_input:
            # Send the user input to the Flask server
            response = requests.post('http://localhost:6000/NYC_Txai_2024', json={"message": user_input})
            
            if response.status_code == 200:
                result = response.json()
                st.markdown(f"<div class='chatbot-response'><strong>chatbot:</strong> {result['response']}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='error-message'><strong>Error:</strong> {response.json().get('error', 'Unknown error')}</div>", unsafe_allow_html=True)
        else:
            st.write("Please enter a message.")

if __name__ == "__main__":
    main()