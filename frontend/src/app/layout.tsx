import "./globals.css";

export const metadata = {
  title: "PM Liquidity Lab",
  description: "Gamma search + liquidity metrics streaming"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="mx-auto max-w-5xl p-6">
          <header className="mb-6 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="h-3 w-3 rounded-full bg-emerald-400" />
              <div className="text-lg font-semibold">PM Liquidity Lab</div>
            </div>
            <div className="text-xs text-zinc-400">
              Frontend (Next.js) -> Backend API at{" "}
              <span className="font-mono">
                {process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000"}
              </span>
            </div>
          </header>
          {children}
        </div>
      </body>
    </html>
  );
}
