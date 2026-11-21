import glob from 'fast-glob'
import { type Metadata } from 'next'
import localFont from 'next/font/local'
import { Providers } from '@/app/providers'
import { Layout } from '@/components/Layout'
import { type Section } from '@/components/SectionProvider'
import PlausibleProvider from 'next-plausible'
import '@/styles/tailwind.css'
import '@/styles/mermaid.css'

const schibstedGrotesk = localFont({
  src: '../../node_modules/@fontsource-variable/schibsted-grotesk/files/schibsted-grotesk-latin-wght-normal.woff2',
  weight: '400 900',
  variable: '--font-schibsted',
  display: 'swap',
})

const geistMono = localFont({
  src: '../../node_modules/@fontsource-variable/geist-mono/files/geist-mono-latin-wght-normal.woff2',
  weight: '400 600',
  variable: '--font-geist-mono',
  display: 'swap',
})

export const metadata: Metadata = {
  title: {
    template: '%s - ZeroFS Documentation',
    default: 'ZeroFS Documentation',
  },
  description:
    'Install, configure, and operate ZeroFS with object storage, filesystem mounts, client libraries, and block devices.',
  icons: {
    icon: [
      {
        url: '/docs/images/favicons/browser.png',
        type: 'image/png',
        sizes: '32x32',
      },
      {
        url: '/docs/images/favicons/Android.png',
        type: 'image/png',
        sizes: '196x196',
      },
    ],
    apple: [
      {
        url: '/docs/images/favicons/iPhone.png',
        type: 'image/png',
        sizes: '180x180',
      },
    ],
  },
}

export default async function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  let pages = await glob('**/*.mdx', { cwd: 'src/app' })
  let allSectionsEntries = (await Promise.all(
    pages.map(async (filename) => [
      '/' + filename.replace(/(^|\/)page\.mdx$/, ''),
      (await import(`./${filename}`)).sections,
    ]),
  )) as Array<[string, Array<Section>]>
  let allSections = Object.fromEntries(allSectionsEntries)

  return (
    <html
      lang="en"
      className={`h-full ${schibstedGrotesk.variable} ${geistMono.variable}`}
      suppressHydrationWarning
    >
      <head>
        <PlausibleProvider
          domain="zerofs.net"
          customDomain="https://p.merklemap.com"
        />
      </head>

      <body className="flex min-h-full bg-zinc-50 antialiased dark:bg-zinc-950">
        <Providers>
          <div className="w-full">
            <Layout allSections={allSections}>{children}</Layout>
          </div>
        </Providers>
      </body>
    </html>
  )
}
