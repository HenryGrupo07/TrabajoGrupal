import streamlit as st
import pandas as pd
from PIL import Image



#st.set_page_config(layout="wide")
st.set_page_config(page_title="PCA", layout="wide", page_icon="馃捑")

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    
image = Image.open('scr/STREAMLIT.png')
felix = Image.open('scr/Felix.png')
sole = Image.open('scr/Sole.png')
Jacque = Image.open('scr/Jac.png')
cesar = Image.open('scr/Cesar.png')
franco = Image.open('scr/Franco.png')
####################
### INTRODUCTION ###
####################
col13,col14= st.columns([1,5])
with col13:
    st.write("")
with col14:    
    st.image(image,width=900)


row1_spacer1, row1_1, row1_spacer2, row1_2, row1_spacer3 = st.columns((.1, 0.8, .1, 2.3, .1))
#row3_spacer1, row3_1, row3_spacer2 = st.columns((.1, 3.2, .1))
with row1_1:
    st.markdown('<h2 style="text-align: justify; color: white"></h2>', unsafe_allow_html=True)
    st.markdown('<h2 style="text-align: justify;color: white"></h2>', unsafe_allow_html=True)
    st.markdown('<h2 style="text-align: center;color: white">驴Qui茅nes somos?</h2>', unsafe_allow_html=True)
    #st.markdown('<h2 style="text-align: center;">somos?</h2>', unsafe_allow_html=True)
with row1_2:
    st.markdown('<h4 style="text-align: justify;color: white">Somos un grupo de profesionales con amplia experiencia en an谩lisis de datos, procesos empresariales y transformaci贸n digital en Latinoam茅rica. Provenientes de 谩reas variadas, pero con la misma vocaci贸n y pasi贸n por la mejora de procesos de negocio</h4>', unsafe_allow_html=True)
    st.markdown('<h4 style="text-align: justify;color: white">Ofrecemos a nuestros clientes una variedad de servicios para asegurar que sus decisiones se basen en informaci贸n precisa y veraz. Esto nos permite brindarles una visi贸n integral para ayudar a las personas en la comprensi贸n de su propio negocio y en la adopci贸n de las mejores estrategias de gesti贸n.</h4>', unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

with col2:
    st.markdown('<h2 style="text-align: center;color: white">SERVICIOS</h2>', unsafe_allow_html=True)

col4, col5, col6 = st.columns(3)
with col4:
    st.image("https://www.esan.edu.pe/images/blog/2015/06/18/data-warehouse-mart-figura-principal.jpg",width=370)
    st.markdown('<h3 style="text-align: center;color: white">Extracci贸n de Datos</h3>', unsafe_allow_html=True)
    st.markdown('<h5 style="text-align: justify;color: white">Recolectamos grandes vol煤menes de datos desde diversas fuentes y los almacenamos en una 煤nica base de datos para su posterior procesamiento y an谩lisis.</h5>', unsafe_allow_html=True)

with col5:
    st.image("https://h3d9f2s8.rocketcdn.me/wp-content/uploads/2022/08/Power-BI-Dashboard.png",width=370)
    st.markdown('<h3 style="text-align: center;color: white">Visualizaci贸n de Datos</h3>', unsafe_allow_html=True)
    st.markdown('<h5 style="text-align: justify;color: white">Combinamos la exploraci贸n y el an谩lisis de los datos con herramientas de visualizaci贸n que nos permitan identificar tendencias y patrones para revelar insights de valor</h5>', unsafe_allow_html=True)

with col6:
    st.image("https://futureoflife.org/wp-content/uploads/2018/12/neural-pathways.jpg",width=340)
    st.markdown('<h3 style="text-align: center;color: white">Machine Learning</h3>', unsafe_allow_html=True)
    st.markdown('<h5 style="text-align: justify;color: white">Recomendamos en funci贸n a las necesidades del negocio el/los algoritmos de machine learning que ayuden a la toma de decisiones e implementamos dicha soluci贸n</h5>', unsafe_allow_html=True)

col7, col8, col9 = st.columns(3)
with col8:
    st.markdown('<h2 style="text-align: center;color: white">PROYECTOS</h2>', unsafe_allow_html=True)
    
row2_spacer1, row2_1, row2_spacer2 = st.columns((.2, 7.1, .2))   
with row2_1:    
    st.markdown('<h3 style="text-align: center;color: white">An谩lisis de Reviews de Google Sector Hotelero de EEUU</h3>', unsafe_allow_html=True)
    st.markdown('<h5 style="text-align: justify;color: white">PCA DATA procesa y analiza de manera eficiente la informaci贸n proveniente de las rese帽as de los usuarios del sector hotelero de Estados Unidos para que de esta manera los gerentes puedan tomar decisiones eficientes en materia de inversi贸n y gesti贸n. Ayud谩ndolos a elegir ubicaciones valiosas para instalar nuevas dependencias y por lo tanto disminuir riesgos de inversi贸n as铆 como mejorar la calidad de sus servicios y ofrecer mejores experiencias para los usuarios, obteniendo de 茅sta manera una ventaja competitiva</h5>', unsafe_allow_html=True)
    st.markdown('<h4 style="text-align: center;color: white">Repositorio en <a href="https://github.com/HenryGrupo07/TrabajoGrupal">Hotel Recommendation</a></h4>', unsafe_allow_html=True)
    

col10, col11, col12 = st.columns(3)
with col11:
    st.markdown('<h2 style="text-align: center;color: white">EQUIPO</h2>', unsafe_allow_html=True)

col13, col14, col15, col16, col17 = st.columns(5)
with col13:
    st.image(sole,width=100)
    st.markdown('<h5 style="text-align: left;color: white"><a href="https://www.linkedin.com/in/m-soledad-gonzalez-data" rel="nofollow noreferrer"><img src="https://i.stack.imgur.com/gVE0j.png" alt="linkedin"> Soledad Gonzalez</a></h5>', unsafe_allow_html=True)
with col14:
    st.image(Jacque,width=100)
    st.markdown('<h5 style="text-align: left;color: white"><a href="https://www.linkedin.com/in/jacqueline-dominguez-51191420/" rel="nofollow noreferrer"><img src="https://i.stack.imgur.com/gVE0j.png" alt="linkedin"> Jacqueline Dominguez</a></h5>', unsafe_allow_html=True)
with col15:
    st.image(franco,width=100)
    st.markdown('<h5 style="text-align: left;"><a href="https://www.linkedin.com/in/francosoto/" rel="nofollow noreferrer"><img src="https://i.stack.imgur.com/gVE0j.png" alt="linkedin"> Franco Soto</a></h5>', unsafe_allow_html=True)
with col16:
    st.image(felix,width=100)
    st.markdown('<h5 style="text-align: left;color: white"><a href="https://www.linkedin.com/in/felixwongp/" rel="nofollow noreferrer"><img src="https://i.stack.imgur.com/gVE0j.png" alt="linkedin"> Felix Wong</a></h5>', unsafe_allow_html=True)
with col17:
    st.image(cesar,width=100)
    st.markdown('<h5 style="text-align: left;color: white"><a href="https://www.linkedin.com/in/cesar-augusto-quinayas-burgos-544084243/" rel="nofollow noreferrer"><img src="https://i.stack.imgur.com/gVE0j.png" alt="linkedin"> C茅sar Quinay谩s</a></h5>', unsafe_allow_html=True)

st.markdown('<h3 style="text-align: center; color: white">馃捁 Nuestro equipo est谩 a su disposici贸n para ayudarle a crear valor para su negocio 馃捁</h3>', unsafe_allow_html=True)

