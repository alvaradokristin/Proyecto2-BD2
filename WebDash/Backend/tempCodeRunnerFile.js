app.get("/", async function (req, res) {
  const response = await getData();
  const data = response.data; // Access the response data