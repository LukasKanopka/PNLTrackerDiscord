import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { RouterProvider, createBrowserRouter } from 'react-router-dom'
import './index.css'
import { Layout } from './components/Layout'
import { RunsPage } from './pages/RunsPage'
import { RunPage } from './pages/RunPage'
import { UserPage } from './pages/UserPage'

const router = createBrowserRouter([
  {
    path: '/',
    element: <Layout />,
    children: [
      { index: true, element: <RunsPage /> },
      { path: 'runs/:runId', element: <RunPage /> },
      { path: 'runs/:runId/users/:author', element: <UserPage /> },
    ],
  },
])

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
)
