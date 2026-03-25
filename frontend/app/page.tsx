'use client'

import { useTheme } from 'next-themes'
import { useState, useEffect, useRef } from 'react'
import { useRouter } from 'next/navigation'
import styles from './home.module.css'

export default function HomePage() {
  const router = useRouter()
  const [query, setQuery] = useState('')

  const { theme, setTheme, systemTheme } = useTheme()
  const currentTheme = theme === 'system' ? systemTheme : theme

  const dialogRef = useRef<HTMLDialogElement>(null)
  const open = () => {dialogRef.current?.showModal()}

  const [mounted, setMounted] = useState(false)
  useEffect(() => setMounted(true), [])
  if (!mounted) return null

  const submit = () => {
    if (query.trim().length === 0) return
    router.push('/rag?q=' + encodeURIComponent(query.trim()))
  }

  return (
    <div className={styles.mainContainer}>
      <div className={styles.branding}>
        <div className={styles.oma}>OMA</div>
        <div className={styles.rag}>RAG</div>
      </div>

      <div className={styles.searchBarContainer}>
        <div className={styles.searchBox}>
          <div className={styles.inputWrap}>
            <input
              type="text"
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') submit() }}
              placeholder="Ask about OMA's projects"
            />
          </div>
          <div className={styles.buttonsContainer}>
            <button className={styles.imgIcon} onClick={submit} />
          </div>
        </div>
      </div>

      <div className={styles.iconsContainer}>
        <div className={styles.iconsTop}>
          <span
            className={styles.themeIcon}
            onClick={() => setTheme(currentTheme === 'dark' ? 'light' : 'dark')}
          />
        </div>
        <span className={styles.info} onClick={open}>Info</span>
      </div>

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
            <br/><br/>&nbsp;&nbsp;• <a href="https://oma.paraclet.io/rag?q=Explain%20the%20design%20approach%20behind%20the%20Mushroom%20Pavilion" rel="noopener noreferrer">Explain the design approach behind the Mushroom Pavilion</a>
            <br/>&nbsp;&nbsp;• <a href="https://oma.paraclet.io/rag?q=List%20masterplan%20projects%20in%20the%20Netherlands%20from%20the%201990s" rel="noopener noreferrer">List masterplan projects in the Netherlands from the 1990s</a>
            <br/>&nbsp;&nbsp;• <a href="https://oma.paraclet.io/rag?q=What%20is%20the%20residential%20project%20in%20France%20with%20an%20elevator%20platform%3F" rel="noopener noreferrer">What is the residential project in France with an elevator platform?</a>
            <br/><br/>Powered by <a href="https://paraclet.io" target="_blank" rel="noopener noreferrer">Paraclet</a>, 2026.
          </p>
        </dialog>
    </div>
  )
}
