import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Project Chronos",
  description: "The AI-native application for video content.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
