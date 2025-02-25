# Gunakan Python 3.11.4 slim-buster sebagai base image untuk ukuran yang lebih kecil
FROM python:3.11.4-slim-buster

# Tetapkan direktori kerja di dalam container
WORKDIR /app

# Salin file requirements.txt dan instal dependensi
# Langkah ini dipisahkan untuk memanfaatkan Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin kode sumber aplikasi
COPY api.py .

# Buka port 6000 untuk akses dari luar container
EXPOSE 6000

# Perintah untuk menjalankan aplikasi saat container dimulai
CMD ["python", "api.py"]