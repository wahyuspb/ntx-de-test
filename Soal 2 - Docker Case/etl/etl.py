import asyncio
import httpx

sentences = [
    "Saya suka makan nasi goreng.",
    "Hari ini cuaca sangat panas.",
    "Anjing itu sangat lucu.",
    "Kami akan pergi ke pantai besok.",
    "Apakah kamu punya hobi?",
    "Buku ini sangat menarik untuk dibaca.",
    "Sekarang waktunya istirahat.",
    "Kucing itu tidur di bawah meja.",
    "Makanan favorit saya adalah rendang.",
    "Kami berencana untuk berlibur ke Bali.",
]

api_url = "http://api:6000/predict"


async def main():
    for sentence in sentences:
        try:
            async with httpx.AsyncClient() as aclient:
                response = await aclient.post(api_url, params={"text": sentence})

                response.raise_for_status()

                print(response.json())
        except Exception as e:
            print(e)
            continue


if __name__ == "__main__":
    asyncio.run(main())