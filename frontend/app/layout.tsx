/* UI sorrrounding the entire application.
Every code here will be displayed on every page */
import { ThemeProvider } from 'next-themes'
import './globals.css';

export default function RootLayout({ children }: { children: React.ReactNode }){
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <title>OMA-RAG</title>
      </head>
      <body>
        <ThemeProvider>{children}</ThemeProvider>
      </body>
    </html>
  );
}