'use client'

import { Suspense, useState, useEffect, useRef, useCallback } from 'react'
import { useTheme } from 'next-themes'
import { useRouter, useSearchParams } from 'next/navigation'
import Link from 'next/link'
import styles from './rag.module.css'

interface QueryResult {
  answer: string
  citations: string[]
  warnings: string[]
}

const CITATION_RE = /\[(P:[^\]]+|C:[^\]]+)\]/g

function renderAnswer(text: string) {
  const parts: React.ReactNode[] = []
  let last = 0
  let match: RegExpExecArray | null
  while ((match = CITATION_RE.exec(text)) !== null) {
    if (match.index > last) parts.push(text.slice(last, match.index))
    const id = match[1]
    const pMatch = id.match(/^P:(.+)$/)
    if (pMatch) {
      parts.push(
        <a key={match.index} className={styles.citation} href={`https://www.oma.com/projects/${pMatch[1]}`} target="_blank" rel="noopener noreferrer">{id}</a>
      )
    } else {
      const cMatch = id.match(/^C:(.+?)#/)
      if (cMatch) {
        parts.push(
          <a key={match.index} className={styles.citation} href={`https://www.oma.com/projects/${cMatch[1]}`} target="_blank" rel="noopener noreferrer">{id}</a>
        )
      } else {
        parts.push(`[${id}]`)
      }
    }
    last = match.index + match[0].length
  }
  if (last < text.length) parts.push(text.slice(last))
  return parts
}

function RagContent() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const initialQ = searchParams.get('q') ?? ''

  const [query, setQuery] = useState(initialQ)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<QueryResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  const { theme, setTheme, systemTheme } = useTheme()
  const currentTheme = theme === 'system' ? systemTheme : theme

  const dialogRef = useRef<HTMLDialogElement>(null)
  const open = () => dialogRef.current?.showModal()

  const [mounted, setMounted] = useState(false)
  useEffect(() => setMounted(true), [])

  const hasFired = useRef(false)

  const fireQuery = useCallback(async (q: string) => {
    if (!q.trim()) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const res = await fetch('/api/query/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: q.trim() }),
      })
      if (!res.ok) {
        const text = await res.text()
        throw new Error(text || `HTTP ${res.status}`)
      }

      const reader = res.body!.getReader()
      const decoder = new TextDecoder()
      let answer = ''
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const event = JSON.parse(line.slice(6))
          if (event.type === 'token') {
            answer += event.token
            setLoading(false)
            setResult({ answer, citations: [], warnings: [] })
          } else if (event.type === 'done') {
            setResult({ answer, citations: event.citations, warnings: event.warnings })
          }
        }
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [])

  // Auto-fire on mount if ?q= exists
  useEffect(() => {
    if (initialQ && !hasFired.current) {
      hasFired.current = true
      fireQuery(initialQ)
    }
  }, [initialQ, fireQuery])

  const submit = () => {
    if (!query.trim()) return
    router.push('/rag?q=' + encodeURIComponent(query.trim()))
    fireQuery(query.trim())
  }

  if (!mounted) return null

  return (
    <div className={styles.container}>
      <div className={styles.topBar}>
        <Link href="/" className={styles.branding}>OMA</Link>

        <div className={styles.searchBox}>
          <div className={styles.inputWrap}>
            <input
              type="text"
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') submit() }}
              placeholder=""
            />
          </div>
          <div className={styles.buttonsContainer}>
            <button className={styles.imgIcon} onClick={() => setQuery('')} />
          </div>
        </div>

        <div className={styles.iconsContainer}>
          <span
            className={styles.themeIcon}
            onClick={() => setTheme(currentTheme === 'dark' ? 'light' : 'dark')}
          />
        </div>
      </div>

      <div className={styles.content}>
        {loading && (
          <div className={styles.loading}>Retrieving...</div>
        )}

        {error && (
          <div className={styles.error}>{error}</div>
        )}

        {result && (
          <>
            <div className={styles.answer}>{renderAnswer(result.answer)}</div>

            {result.warnings.length > 0 && (
              <div className={styles.warnings}>
                {result.warnings.map((w, i) => (
                  <div key={i} className={styles.warning}>{w}</div>
                ))}
              </div>
            )}
          </>
        )}
      </div>

      <span className={styles.info} onClick={open}>Info</span>

      <dialog
          ref={dialogRef}
          className={styles.dialog}
          onMouseDown={e => {
            if (e.target === e.currentTarget) {
              e.currentTarget.close()
            }
          }}
        >
          <p>
            This app answers one question at a time about OMA and its projects, using only <a href="https://www.oma.com/projects" target="_blank" rel="noopener noreferrer">OMA's project archive</a> as its source.
            <br/><br/>All factual statements are supported by citations. If the archive does not contain enough information, the app will say so.
            <br/><br/>This is not a conversational system. Each query is handled independently.
            <br/><br/>Example questions:
            <br/><br/>&nbsp;&nbsp;• <a href="/rag?q=Explain%20the%20design%20approach%20behind%20the%20Mushroom%20Pavilion" rel="noopener noreferrer">Explain the design approach behind the Mushroom Pavilion</a>
            <br/>&nbsp;&nbsp;• <a href="/rag?q=List%20residential%20projects%20in%20the%20Netherlands%20in%20the%201990s" rel="noopener noreferrer">List residential projects in the Netherlands in the 1990s</a>
            <br/>&nbsp;&nbsp;• <a href="/rag?q=What%20is%20the%20residential%20project%20in%20France%20with%20an%20elevator%20platform%3F" rel="noopener noreferrer">What is the residential project in France with an elevator platform?</a>
            <br/><br/>Powered by <a href="https://paraclet.io" target="_blank" rel="noopener noreferrer">Paraclet</a>, 2026.
          </p>
      </dialog>
    </div>
  )
}

export default function RagPage() {
  return (
    <Suspense>
      <RagContent />
    </Suspense>
  )
}
