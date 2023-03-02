import streamlit as st
from PIL import Image
import streamlit.components.v1 as components


st.set_page_config(layout="wide")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
image = Image.open('scr/STREAMLIT.png')

####################
### INTRODUCTION ###
####################

col1, col2, col3 = st.columns([1,5,1])    
with col2:
    st.markdown('<h3 style="text-align: center;color: white">Análisis de alojamientos en Estados Unidos</h3>', unsafe_allow_html=True)   

    #st.markdown('<iframe title="Power_bi_viernes" width="1350" height="650" src="https://app.powerbi.com/view?r=eyJrIjoiMTc2MTU0MTItM2ExMS00Mzc4LWIzM2QtNDdlOGFhOGZiMDFjIiwidCI6ImU0NmQzODYyLTg1OTUtNDVkMS05YjY5LTYzMDc5OGQ4OTAyZCIsImMiOjR9" frameborder="0" allowFullScreen="true"></iframe>', unsafe_allow_html=True)
components.iframe(src="https://app.powerbi.com/view?r=eyJrIjoiN2JhNDNhNGEtYjA2Ni00Yjk3LTkwN2EtMWM4ZjY1NWY0ZWM5IiwidCI6ImU0NmQzODYyLTg1OTUtNDVkMS05YjY5LTYzMDc5OGQ4OTAyZCIsImMiOjR9", width=1350, height=630, scrolling=True)
    

