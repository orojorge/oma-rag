import { AwsClient } from 'aws4fetch'

const API_URL = process.env.API_URL ?? 'http://localhost:8000'
const isAws = API_URL.includes('.lambda-url.')

const aws = isAws
  ? new AwsClient({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
      region: process.env.AWS_REGION ?? 'eu-central-1',
      service: 'lambda',
    })
  : null

export async function POST(request: Request) {
  const body = await request.json()
  const url = `${API_URL}/query`
  const init = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }

  const res = aws
    ? await aws.fetch(url, init)
    : await fetch(url, init)

  return new Response(await res.text(), {
    status: res.status,
    headers: { 'Content-Type': 'application/json' },
  })
}
