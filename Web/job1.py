import urllib.request, urllib.error, urllib.parse

url = 'https://www.eltiempo.com'

respuesta = urllib.request.urlopen(url)
contenidoWeb = respuesta.read().decode('UTF-8')

print(contenidoWeb[0:100000])